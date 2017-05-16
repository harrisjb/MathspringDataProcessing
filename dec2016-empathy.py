from __future__ import division #makes it so that / is always floating point ("normal") division, and // is integer division
import pymysql #The main package we use to pull data from the database
import xlsxwriter #The package we use to write excel files with formatting and multiple sheets
from scipy.stats.distributions import chi2 #Used for the Likelihood Ratio Test
from datetime import date
from collections import defaultdict, deque
import re, math, os, sys, pickle #these are default library modules

###############################################################################
###                            Configuration Area                           ###
###############################################################################

#Database url, username and password; these are for public read-only access
db_hostname = "rose.cs.umass.edu"
db_dbname = "wayangoutpostdb"
db_username = None
db_password = None
reload_data = True #Set to False if you want to use cached data; this may be faster than querying the database
if os.path.exists("database_user.txt"):
	with open("database_user.txt") as f:
		lines = f.readlines()
		db_username = lines[0].strip()
		db_password = lines[1].strip()

#What to call the output file; by default use the same name as the script
output_file = __file__[:-3] + ".xlsx"
data_folder = __file__[:-3] + "-data"
if not os.path.exists(data_folder):
	os.mkdir(data_folder)

#Info to specify what parts of the event log we want
time_ranges = (
	#Each row here specifies a range of days that the students worked in the system,
	# in YEAR, MONTH, DAY format
	(date(2016, 12, 1), date(2016, 12, 2)),
	(date(2016, 12, 8), date(2016, 12, 9)),
)
classes = (#The class IDs for this trial
	1284, #Period 2
	1285, #Period 3
	1286, #Period 5
)
exclude_student_ids = (
	#Someone typically goes in and tests the system as a user in the class
	# (e.g. Ivon), make sure we exclude these users so we get only the real students
	# Note that we automatically exclude students with:
	#	trialUser set to 1
	#	usernames containing "test"
	
	#These students had very little time spent or very few problems answered
	36787,
	36788,
	36833
)

#A file containing tab-based columns of studentId, testType (pretest, posttest), probId, and isCorrect
#This is used for grading pre/post test problems that the system can't do automatically (e.g. open-ended ones)
# If we don't have an entry for a particular question/answer then it will use the system's grading
prepost_corrections_file = "dec2016_prepost_grading_corrections.tsv"

#There MIGHT be issues someone creates a username containing "test", e.g. "foltest"
# in which case this will need to be adjusted

###############################################################################
###                          End Configuration Area                         ###
###############################################################################

#Connect to the database
cursor = None
if db_username is not None and db_password is not None:
	print "Connecting to the database..."
	connection = pymysql.connect(host=db_hostname, user=db_username, passwd=db_password, db=db_dbname, charset="latin1")
	cursor = connection.cursor()
	print "Connected, building queries..."
else:
	print "Using cached data in the %s folder..." % data_folder

#Just runs a query and dumps it into a Python list of row-tuples
def query(name, q):
	data_filename = os.path.join(data_folder, name + ".pickle")
	if cursor is not None and reload_data:
		result = []
		cursor.execute(q)
		for row in cursor.fetchall():
			result.append([elt.strip() if hasattr(elt, "strip") else elt for elt in row])
		with open(data_filename, 'wb') as f:
			pickle.dump(result, f)
		return result
	elif os.path.exists(data_filename):
		with open(data_filename, 'rb') as f:
			return pickle.load(f)
	else:
		print "Error: You must have either a database login or the data file %s" % data_filename
		sys.exit(0)

def getHeaders(table_name):
	headers = query(table_name + "_headers", "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'" % table_name)
	headers = tuple(row[0] for row in headers)
	h = {name: c for c,name in enumerate(headers)}
	return headers, h

#This is the subquery for getting the student ids relevant to the study
student_query = "SELECT id,pedagogyId FROM student WHERE " \
				+ "trialUser = 0 AND userName NOT LIKE '%test%' " \
				+ " AND classId IN " + str(classes)
if len(exclude_student_ids) > 0:
	student_query += "AND id NOT IN " + str(exclude_student_ids)

student_data = query("student_ids", student_query)
student_ids = tuple(i[0] for i in student_data)

problem_difficulty_data = query("problem_difficulties", "SELECT id,cachedProbDifficulty FROM problem WHERE id IN"
		"(SELECT DISTINCT(problemId) FROM eventlog WHERE studId IN " + str(student_ids) + ");")
problem_difficulty = {}
for probId,difficulty in problem_difficulty_data:
	problem_difficulty[probId] = difficulty
	
#Build the query to select just the relevant rows from the eventlog
eventlog_query = "SELECT * FROM eventlog WHERE studId IN " + str(student_ids) + " "
#Build the time-constraint part of the query
if len(time_ranges) > 0: #if any time ranges were specified
	eventlog_query += " AND ("
for i,(start,end) in enumerate(time_ranges):
	if i > 0: #if it's not the first time, we need to add an "OR"
		eventlog_query += " OR "
	eventlog_query += "(DATE(time) >= DATE('" + str(start) + "') AND DATE(time) <= DATE('" + str(end) + "'))"
if len(time_ranges) > 0:
	eventlog_query += ")"
	
eventlog_query += " ORDER BY studId ASC, time ASC;"

print "Running the query to get the event log data..."

#Pull the event log data
eventlog = query("eventlog", eventlog_query)

#We also want the names of the columns, pull those now
eventlog_headers, h = getHeaders("eventlog")

print "Cleaning the data a bit..."

#Define some functions and tables to do various processing steps

#This just lets just check for a bunch of different pieces of text at once
# we look for the first string, and put the second in the EventType column
event_map = {
	'demo':'Demo',
	'Collaboration':'Collab',
	'AskEmotionIntervention-':'EmoReport',
	'TopicIntro':'TopicIntro',
	'BeginProblem':'Problem',
	'Home':'Home',
	'MyProgressPage':'MPP',
	'MPPContinueTopic':'MPP',
}
#Checks if any of the events in event_map occur in the provided raw_event,
# which will be pulled from the "activityName" and "action" columns
#If so, it returns the corresponding nice event name for the "EventType" column
def checkForEvent(raw_event):
	for trigger,event in event_map.items():
		if trigger in raw_event:
			return event
	return None


#Figure out if this represents a student finishing a problem
def updateUniqueEndProb(unique_end_prob, session_num, row, activity, last_activity, action, last_action):
	updated = False
	if row[h["sessNum"]] != session_num \
	or ("demo" in activity and "demo" not in last_activity) \
	or "Collaboration" in activity \
	or "AskEmotionIntervention-" in activity \
	or ("TopicIntro" in activity and "TopicIntro" not in last_activity) \
	or ("BeginProblem" in action and "demo" not in activity) \
	or "EndProblem" in last_action \
	or "Home" in action \
	or "MyProgressPage" in action \
	or "MPPContinueTopic" in action:
		unique_end_prob += 1
		updated = True
	return unique_end_prob, updated

#Figure out what type of event this row describes
def updateEventType(event_type, session_num, row, activity, action):
	if session_num is None or row[h['sessNum']] !=  session_num:
		event_type = "NewSess"
		session_num = row[h['sessNum']]
	else:
		event_type = checkForEvent(activity) \
				  or checkForEvent(action) \
				  or event_type
	return event_type, session_num

#This just flips the mapping so it's ID -> Category for faster processing later
def flipTupleDict(tuple_dict):
	flipped = {}
	for cat,tup in tuple_dict.items():
		for val in tup:
			flipped[val] = cat
	return flipped

#Used for several different processing steps
last_activity = ""
last_action = ""
session_num = None

#Used for tracking between rows
unique_end_prob = 0
last_attempt_unique_end_prob = 0
event_type = None
				  
#Metrics for sanity checks
days = set()

#Per-student metrics for later analysis
lc_message_map_inv = dict(
	Empathy = ('interestHigh','frustrationLow','frustratedCombo2','frustratedCombo1','confidenceHigh','anxiousCombo2','anxiousCombo1'),
	GrowthMindset = ('noEffortAttribution2','incorrectNoEffort2','incorrectNoEffort1','incorrectEffort2','incorrectEffort1','incorrectAttribution5',
		'incorrectAttribution3','incorrectAttribution2','incorrectAttribution1','generalAttribution1','generalAttribution2','generalAttribution3',
		'generalAttribution4','generalAttribution5','correctEffort2','correctEffort1',),
	SuccessFailure = ('incorrect1','incorrect2','incorrect3','incorrect4','incorrect5','incorrect6', 'correct1', 'correct2', 'correct3', 'correct4', 
		'correct5', 'correct6')
)
lc_message_map = flipTupleDict(lc_message_map_inv)
student_metrics = {}
student_timeseries_answer_metrics = {}
student_timeseries_emotion_metrics = {}
seen_attempt = False
last_three_answers = None
last_three_hints = None
hints_in_problem = None
incorrect_attempts = None

def updateAnswerMetrics(ametrics, metrics, row):
	timeOnProblem = float(row[h["probElapsed"]])/60000 #convert from ms to min
	ametrics["TimeOnProblem"] = timeOnProblem
	last_three_hints.append(len(hints_in_problem))
	ametrics["TotalHints"] = metrics["NumHints"]
	ametrics["HintsInProblem"] = len(hints_in_problem)
	ametrics["HintsLast3"] = sum(last_three_hints)/len(last_three_hints)
	ametrics["CurrentHints"] = last_three_hints[-1]
	ametrics["LastHints"] = last_three_hints[-2] if len(last_three_hints) > 1 else None
	ametrics["CurrentCorrect"] = last_three_answers[-1]
	ametrics["LastCorrect"] = last_three_answers[-2] if len(last_three_answers) > 1 else None
	ametrics["LastIncorrectAttempts"] = incorrect_attempts[-2] if len(incorrect_attempts) > 1 else 0
	ametrics["CurrentIncorrectAttempts"] = incorrect_attempts[-1]

#Run through and process the event log
for i in range(len(eventlog)):
	row = eventlog[i]
	
	#Building some of the metrics to print out for a sanity check
	days.add(row[h["time"]].date())
	
	activity = row[h["activityName"]] or ""
	action = row[h["action"]] or ""
	unique_end_prob, updated = updateUniqueEndProb(unique_end_prob, session_num, row, activity, last_activity, action, last_action)
	event_type, session_num = updateEventType(event_type, session_num, row, activity, action)

	#Store these for the next row's unique_end_prob check
	last_activity = activity
	last_action = action
	
	#Update the row with the new columns
	eventlog[i] = [event_type, unique_end_prob, i] + row
	
	#Extract some per-student metrics from this row
	studId = row[h["studId"]]
	if studId not in student_metrics:
		if seen_attempt: #this means the student ended their session without a final EndProblem
			prev_row = eventlog[i-1][3:] #have to prune out the things we added so the indices aren't messed up
			prev_studId = prev_row[h["studId"]]
			updateAnswerMetrics(student_timeseries_answer_metrics[prev_studId][-1],
								student_metrics[prev_studId], prev_row)
		seen_attempt = False
		hints_in_problem = set()
		last_three_answers = deque(maxlen=3)
		last_three_hints = deque(maxlen=3)
		incorrect_attempts = deque(maxlen=2)
		student_metrics[studId] = defaultdict(lambda: 0)
		student_timeseries_answer_metrics[studId] = []
		student_timeseries_emotion_metrics[studId] = dict(Confidence=[],Frustration=[])
	metrics = student_metrics[studId]
	if action == "EndProblem":
		metrics["TimeInTutor"] += float(row[h["probElapsed"]])/60000 #convert from ms to min
	elif action == "Attempt":
		isCorrect = row[h["isCorrect"]]
		if unique_end_prob != last_attempt_unique_end_prob:
			last_attempt_unique_end_prob = unique_end_prob #mark this problem as attempted
			metrics["AvgProblemDifficulty"] += problem_difficulty[row[h["problemId"]]]
			last_three_answers.append(isCorrect)
			metrics["CorrectTotal" if isCorrect == 1 else "IncorrectTotal"] += 1
			incorrect_attempts.append(0)
		if isCorrect == 0:
			metrics["TotalIncorrectAttempts"] += 1
			incorrect_attempts[-1] += 1
	elif action == "Hint":
		hintId = row[h["hintId"]]
		if hintId not in hints_in_problem:
			hints_in_problem.add(hintId)
			metrics["NumHints"] += 1
	
	#Extract some time-series metrics about the student;
	# a few of these are collected above in the per-student metrics
	if not seen_attempt and action == "Attempt":
		seen_attempt = True
		ametrics = defaultdict(lambda: 0)
		ametrics["studId"] = studId
		#get the current time used on previous problems plus the time on this current problem
		timeOnProblem = float(row[h["probElapsed"]])/60000 #convert from ms to min
		ametrics["TimeInTutor"] = metrics["TimeInTutor"] + timeOnProblem
		ametrics["TimeToFirst"] = timeOnProblem
		for metric in ("Total Messages", "Empathy Messages", "GrowthMindset Messages",
						"SuccessFailure Messages", "CorrectTotal", "IncorrectTotal"):
			ametrics[metric] = metrics[metric]
		if len(student_timeseries_answer_metrics[studId]) > 0:
			last_answer_metrics = student_timeseries_answer_metrics[studId][-1]
			for message_type in ("Empathy", "GrowthMindset", "SuccessFailure"):
				delta = ametrics[message_type + " Messages"] - last_answer_metrics[message_type + " Messages"]
				ametrics["Last Problem " + message_type] = 1 if delta > 0 else 0
			
		correctlast3 = 0
		incorrectlast3 = 0
		for attempt in last_three_answers:
			if attempt == 1:
				correctlast3 += 1
			else:
				incorrectlast3 += 1
		ametrics["CorrectLast3"] = correctlast3
		ametrics["IncorrectLast3"] = incorrectlast3
		ametrics["ProblemDifficulty"] = problem_difficulty[int(row[h["problemId"]])]
		student_timeseries_answer_metrics[studId].append(ametrics)
	elif seen_attempt and action == "EndProblem":
		seen_attempt = False
		updateAnswerMetrics(student_timeseries_answer_metrics[studId][-1], metrics, row)
		hints_in_problem = set()
		
	userInput = row[h["userInput"]]
	if event_type == "EmoReport" and action == "InputResponse" and userInput:
		m = re.search('<emotion name="([^"]*)"\\s*level="([^"]*)"', userInput)
		if m:
			emotion = m.group(1)
			response = int(m.group(2))
			if response >= 1 and response <= 5:
				emetrics_list = student_timeseries_emotion_metrics[studId][emotion]
				last_emetrics = emetrics_list[-1] if len(emetrics_list) > 0 else defaultdict(lambda: 0)
				emetrics = defaultdict(lambda: 0)
				emetrics["studId"] = studId
				emetrics[emotion] = response
				#For these, find the increase since the last record
				for metric in ("AvgProblemDifficulty", "CorrectTotal", "IncorrectTotal", "TotalIncorrectAttempts",
								"Empathy Messages", "GrowthMindset Messages", "SuccessFailure Messages"):
					emetrics[metric] = metrics[metric] - last_emetrics[metric]
				#can't average yet, because we need to use the raw value to get the diff for the next entry
				# emetrics["AvgProblemDifficulty"] /= max(1, emetrics["CorrectTotal"] + emetrics["IncorrectTotal"])
				emetrics_list.append(emetrics)
	
	#Delay updating this until after the other metrics are recorded,
	# because it technically plays after the action we're recording
	emotion = row[h["emotion"]]
	if emotion in lc_message_map:
		metrics[lc_message_map[emotion] + " Messages"] += 1
		metrics["Total Messages"] += 1
	
#These are the columns we generated with the logic above
generated_headers = ("EventType", "UniqueEndProb", "newId")

print "We have %d event log rows covering %d classes, %d students, on %d separate days." % \
	(len(eventlog), len(classes), len(student_metrics), len(days))

print "Putting the event log into a sheet..."
#Create an Excel workbook
workbook = xlsxwriter.Workbook(output_file)
sheet_eventlog = workbook.add_worksheet("eventlog")
#Write in the modified eventlog
bold_format = workbook.add_format()
bold_format.set_bold()
sheet_eventlog.write_row(0, 0, generated_headers + eventlog_headers, bold_format)
for i,row in enumerate(eventlog):
	sheet_eventlog.write_row(i+1, 0, row)

prepost_corrections = {}
if prepost_corrections_file is not None:
	print "Reading in the correct grading of pre/post test problems..."
	with open(prepost_corrections_file) as f:
		lines = f.readlines()[1:] #strip out the header column
		for line in lines:
			line = line.replace("\t\t", "\t")
			columns = line.split("\t")
			if len(columns) >= 4:
				studId = int(columns[0].strip())
				test_type = columns[1].strip()
				probId = int(columns[2].strip())
				isCorrect = int(columns[3].strip())
				prepost_corrections[(studId, test_type, probId)] = isCorrect
	
print "Getting pre/post test data..."
#Start building the pre/post test data sheet
sheet_preposttestdata = workbook.add_worksheet("preposttestdata")

preposttest_headers, h = getHeaders("preposttestdata")

#Pull the pre/post test data
preposttestdata = query("preposttestdata", "SELECT * FROM preposttestdata WHERE studId IN " + str(student_ids) + " ORDER BY studId ASC, testType DESC, probId ASC;")

print "Putting the pre/post test data into a sheet..."
sheet_preposttestdata.write_row(0, 0, preposttest_headers, bold_format)
for i,row in enumerate(preposttestdata):
	#Add in the corrected grading if it exists
	studId = int(row[h["studId"]])
	test_type = row[h["testType"]]
	probId = int(row[h["probId"]])
	key = (studId, test_type, probId)
	isCorrect = prepost_corrections[key] if key in prepost_corrections else int(row[h["isCorrect"]])
	row[h["isCorrect"]] = isCorrect
	sheet_preposttestdata.write_row(i+1, 0, row)
	
print "Getting the pre/post test problems..."

sheet_preposttestproblem = workbook.add_worksheet("preposttestproblem")
preposttestproblem_headers = ( #we restrict it to just certain columns because some columns have nasty binary data
	"id", "name", "description", "answer", "ansType", "problemSet",
	"aChoice", "bChoice", "cChoice", "dChoice", "eChoice", "descriptionId"
)
h = {name: c for c,name in enumerate(preposttestproblem_headers)}

#Pull the question information so that we can see what various question ids are asking
preposttestproblem = query("preposttestproblem", "SELECT " + ", ".join(preposttestproblem_headers) \
		+ " FROM prepostproblem WHERE id IN " \
		+ "(SELECT DISTINCT(id) FROM prepostproblem WHERE id IN " \
		+ "(SELECT probId FROM preposttestdata WHERE studId IN " + str(student_ids) + ")) " \
		+ "ORDER BY id ASC;")

sheet_preposttestproblem.write_row(0, 0, preposttestproblem_headers, bold_format)
for i,row in enumerate(preposttestproblem):
	sheet_preposttestproblem.write_row(i+1, 0, row)

print "Extracting the pre/post test comparison..."

#Define the categories of interest for the "survey"-style problems in the pre/post test	
prepost_categories_inv = dict(
	Interest				= (176,),
	Confusion				= (177,),
	Frustration 			= (178,188),
	Excitement				= (179,),
	PerformanceAvoidance	= (180,186),
	LearningOrientation		= (181,183),
	MathValue				= (185,),
	MathLiking				= (187,),
	PerformanceApproach		= (189, 190),
	#This one is the category for the problems that are used for performance evaluation
	Score					= tuple(range(262, 267)) #everything from 262 to 266 inclusive
)
category_order = ("Interest", "Excitement", "Confusion", "Frustration",
					"PerformanceAvoidance", "PerformanceApproach", "LearningOrientation",
					"MathValue", "MathLiking", "Score", "NormalizedLearningGain")

prepost_categories = flipTupleDict(prepost_categories_inv)

performance_approach_answers = {189: set((1,3)), 190: set((2,3))}

#Go through the pre/post test data and extract the average score in each category per student
h = {name: c for c,name in enumerate(preposttest_headers)}
students_prepost = {} #studId -> (test_type -> (category -> [scores]))
num_missing_answers = 0
for row in preposttestdata:
	studId = row[h['studId']]
	test_type = row[h['testType']]
	if studId not in students_prepost:
		students_prepost[studId] = {}
	student = students_prepost[studId]
	if test_type not in student:
		student[test_type] = defaultdict(lambda: [])
	test = student[test_type]
	probId = row[h['probId']]
	if probId in prepost_categories: #if not, then this isn't a survey question
		category = prepost_categories[probId]
		try:
			if category == "Score":
				test[category].append(float(row[h['isCorrect']]))
			elif category == "PerformanceApproach":
				test[category].append(1 if int(row[h['studentAnswer']][0]) in performance_approach_answers[probId] else 0)
			else:
				test[category].append(float(row[h['studentAnswer']][0]))
		except ValueError: #the studentAnswer didn't have a number at the start
			# print(studId, test_type, probId, row[h['studentAnswer']])
			num_missing_answers += 1
			pass #If they don't answer, it will have "I don't know", which we can't use

print "We were missing answers for %d student-question pairs, which we ignored." % num_missing_answers
			
#Get averages for each pedagogy group
pedagogy_groups_inv = dict(Empathy = (1, 2), GrowthMindset = (3, 4), SuccessFailure = (5, 6))
pedagogy_group_order = ("Empathy", "GrowthMindset", "SuccessFailure")
pedagogy_groups = flipTupleDict(pedagogy_groups_inv)
prepost_by_group = {group: {category: [[], []] for category in prepost_categories_inv}
							for group in pedagogy_groups_inv}
num_prepost_students = 0
for studId, pedagogyId in student_data:
	group = prepost_by_group[pedagogy_groups[pedagogyId]]
	if studId not in students_prepost: #we didn't have pre/post data from this student
		continue
	student = students_prepost[studId]
	if 'pretest' in student and 'posttest' in student: #exclude students that didn't do both
		num_prepost_students += 1
		pedagogy = pedagogy_groups[pedagogyId]
		student_metrics[studId]["Pedagogy"] = pedagogy
		student_metrics[studId]["AvgProblemDifficulty"] /= student_metrics[studId]["CorrectTotal"] + student_metrics[studId]["IncorrectTotal"]
		for answer_metric in student_timeseries_answer_metrics[studId]:
			answer_metric["Pedagogy"] = pedagogy
		for emotion, emotion_metrics in student_timeseries_emotion_metrics[studId].items():
			for emotion_metric in emotion_metrics:
				emotion_metric["Pedagogy"] = pedagogy
		for test_type,categories in student.items():
			for category,answers in categories.items():
				student_average = None
				if len(answers) > 0: #discard students who didn't answer
					student_average = sum(answers)/len(answers)
					group[category][0 if test_type == "pretest" else 1].append(student_average)
				student_metrics[studId][str(test_type + " " + category)] = student_average
				# else:
					# num_missing_answers += 1
		pre_score = student_metrics[studId]['pretest Score']
		post_score = student_metrics[studId]['posttest Score']
		learning_gain = post_score - pre_score
		normalized_learning_gain = learning_gain / (1 - pre_score) if pre_score < 1 else None
		student_metrics[studId]["LearningGain"] = learning_gain
		student_metrics[studId]["NormalizedLearningGain"] = normalized_learning_gain
	else:
		del student_timeseries_answer_metrics[studId]
					
print "We had %d students do both the pretest and posttest." % num_prepost_students
					
#Convert the list of per-student averages to per-group averages
for group, categories in prepost_by_group.items():
	for category, tests in categories.items():
		for i in [0,1]:
			tests[i] = sum(tests[i]) / len(tests[i])
		tests.append(tests[1] - tests[0]) #add a pre/post difference
	#also do normalized learning gain
	scores = categories["Score"]
	normalized_learning_gain = (scores[1] - scores[0]) / (1 - scores[0])
	categories["NormalizedLearningGain"] = ["", "", normalized_learning_gain]

#Create a sheet for this pre/post summary data
sheet_prepostsummary = workbook.add_worksheet("prepostsummary")
sheet_prepostsummary.write_column(0, 0, ("Group", "Test") + tuple(category for category in category_order), bold_format)
col_num = 1
test_labels = ("pretest", "posttest", "difference")
for group in pedagogy_group_order:
	categories = prepost_by_group[group]
	for i in range(3):
		category_averages = tuple(categories[category][i] for category in category_order)
		sheet_prepostsummary.write_column(0, col_num+i, (group, test_labels[i]), bold_format)
		sheet_prepostsummary.write_column(2, col_num+i, category_averages, bold_format if i == 2 else None)
	col_num += 3

#This isn't in the database, so I had to manually copy it from the Teacher Tools pre/post report
learning_estimation	= {	36751: 38.9, 36745: 16.7, 36754: 61.1, 36743: 23.3, 36753: 33.3, 36752: 19, 36746: -2.1,
						36756: 4.8, 36747: 16.7, 36749: 4.2, 36748: -9.5, 36790: 8.3, 36785: 33.3, 36786: 75,
						36772: 33.3, 36779: 25, 36774: 22.2, 36769: 33.3, 36789: 0, 36775: 0, 36784: 33.3,
						36770: 33.3, 36781: 16.7, 36792: -5.6, 36777: 16.7, 36771: 8.3, 36776: -11.1, 36780: 16.7,
						36773: -8.3, 36794: -66.7, 36824: 33.3, 36828: 11.1, 36819: 25, 36830: 75, 36838: -8.3,
						36815: 8.3, 36834: 16.7, 36831: 0, 36829: 8.3, 36826: 8.3, 36820: -16.7, 36832: -8.3,
						36822: 29.2, 36837: -16.7, 36839: 50, 36818: 10, 36825: 6.7, 36835: -5.6, 36827: 11.1,
						36816: 5.6, 36823: 5.6, 36821: -66.7, 36836: -16.7}

#Create a student metrics sheet
student_metric_labels = ["Pedagogy",
	"Total Messages", "Empathy Messages", "GrowthMindset Messages", "SuccessFailure Messages"]
for message_type in ("Empathy", "GrowthMindset", "SuccessFailure"):
	student_metric_labels.append("%" + message_type + " Messages")
student_metric_labels += ["TimeInTutor", "CorrectTotal", "IncorrectTotal", "NumHints", "AvgProblemDifficulty", "TotalIncorrectAttempts"]
exclude_prepost = set(("NormalizedLearningGain", "LearningGain"))
for category in category_order:
	if category not in exclude_prepost:
		for test_type in ("pretest", "posttest"):
			student_metric_labels.append(str(test_type + " " + category))
student_metric_labels.append("LearningGain")
student_metric_labels.append("NormalizedLearningGain")
student_metric_labels.append("LearningEstimation")

sheet_studentmetrics = workbook.add_worksheet("studentmetrics")
sheet_studentmetrics.write_row(0, 0, ["StudentId"] + student_metric_labels, bold_format)
i = 0
for student,metrics in student_metrics.items():
	i += 1
	row = [student]
	#Add in learning estimation
	metrics["LearningEstimation"] = learning_estimation[student] if student in learning_estimation else None
	#Convert message counts to percentages
	for message_type in ("Empathy", "GrowthMindset", "SuccessFailure"):
		metrics["%" + message_type + " Messages"] = metrics[message_type + " Messages"] / metrics["Total Messages"]
	for label in student_metric_labels:
		row.append(metrics[label])
	sheet_studentmetrics.write_row(i, 0, row)

#Create a student timeseries answer metrics sheet
sheet_student_answer_metrics = workbook.add_worksheet("studentanswermetrics")
student_answer_metric_labels = ["studId", "Pedagogy", "ProblemDifficulty",
	"TimeInTutor", "TimeToFirst", "TimeOnProblem", "LastIncorrectAttempts", "CurrentIncorrectAttempts",
	"TotalHints", "HintsLast3", "HintsNext3", "HintsInProblem",
	"Total Messages", "Empathy Messages", "GrowthMindset Messages", "SuccessFailure Messages",
	"CorrectTotal", "IncorrectTotal", "CorrectLast3", "CorrectNext3", "IncorrectLast3",
	"LastCorrect", "CurrentCorrect", "LastHints", "CurrentHints",
	"Last Problem Empathy", "Last Problem GrowthMindset", "Last Problem SuccessFailure"]
sheet_student_answer_metrics.write_row(0, 0, student_answer_metric_labels, bold_format)
i = 0
for _,answer_metrics in student_timeseries_answer_metrics.items():
	#Compute the look-ahead values
	for r,answer_metric in enumerate(answer_metrics[3:]):
		answer_metrics[r]["HintsNext3"] = answer_metric["HintsLast3"]
		answer_metrics[r]["CorrectNext3"] = answer_metric["CorrectLast3"]
	if len(answer_metrics) > 2:
		answer_metrics[-3]["HintsNext3"] = (answer_metrics[-2]["CurrentHints"] + answer_metrics[-1]["CurrentHints"])/2
		answer_metrics[-3]["CorrectNext3"] = answer_metrics[-2]["CurrentCorrect"] + answer_metrics[-1]["CurrentCorrect"]
	if len(answer_metrics) > 1:
		answer_metrics[-2]["HintsNext3"] = answer_metrics[-1]["CurrentHints"]
		answer_metrics[-2]["CorrectNext3"] = answer_metrics[-1]["CurrentCorrect"]
	#Compute the average value for these to fill in the rows where it's undefined
	avg_hints_next_3 = 0
	avg_correct_next_3 = 0
	avg_incorrect_attempts = 0
	for answer_metric in answer_metrics:
		avg_hints_next_3 += answer_metric["HintsNext3"]
		avg_correct_next_3 += answer_metric["CorrectNext3"]
		avg_incorrect_attempts += answer_metric["LastIncorrectAttempts"]
	denominator = max(len(answer_metrics) - 1, 1)
	avg_hints_next_3 /= denominator
	avg_correct_next_3 /= denominator
	avg_incorrect_attempts /= denominator
	answer_metrics[0]["HintsLast3"] = avg_hints_next_3
	answer_metrics[-1]["HintsNext3"] = avg_hints_next_3
	answer_metrics[0]["CorrectLast3"] = avg_correct_next_3
	answer_metrics[-1]["CorrectNext3"] = avg_correct_next_3
	answer_metrics[0]["LastIncorrectAttempts"] = avg_incorrect_attempts
	for answer_metric in answer_metrics:
		i += 1
		sheet_student_answer_metrics.write_row(i, 0, 
			[answer_metric[label] for label in student_answer_metric_labels])

for emotion in ("Confidence", "Frustration"):
	i = 0
	sheet_student_emotion_metrics = workbook.add_worksheet("student" + emotion.lower() + "metrics")
	student_emotion_metric_labels = ["studId", "Pedagogy", emotion, "AvgProblemDifficulty", "CorrectTotal",
		"IncorrectTotal", "Empathy Messages", "GrowthMindset Messages", "SuccessFailure Messages"]
	sheet_student_emotion_metrics.write_row(0, 0, student_emotion_metric_labels, bold_format)
	for _,emotion_metrics in student_timeseries_emotion_metrics.items():
		for emotion_metric in emotion_metrics[emotion]:
			i += 1
			emotion_metric["AvgProblemDifficulty"] /= max(1, emotion_metric["CorrectTotal"] + emotion_metric["IncorrectTotal"])
			sheet_student_emotion_metrics.write_row(i, 0, 
				[emotion_metric[label] for label in student_emotion_metric_labels])

#Save what we've written as a file
print "Writing " + output_file + "..."
workbook.close()

print "\n----------\n"

print "Calculating Markov models for within-tutor emotion self-reports based on pedagogies..."

smoothing = 0.01 #pseudocount smoothing strength

def convertTransitionCountsToLogProbabilities(transitions):
	#normalize the transition probabilities and convert to log-space for numerical stability
	for emotion, pedagogies in transitions.items():
		for pedagogy, trans in pedagogies.items():
			print emotion, pedagogy
			print "Total number of data cases for each transition: ", trans
			for row in trans:
				total = sum(row)
				for i in range(len(row)):
					alpha = total*smoothing / 2
					row[i] = (row[i] + alpha)/(total + 2*alpha) #pseudocount
			print "Transition matrix: ", trans
			#you can derive this steady-state distribution by hand with the system of equations given by:
			# [A] [a b] = [A B]
			# [B] [c d]
			# and A + B = 1
			#steady_emotion is the solution for A
			steady_emotion = 1/(1 + (trans[0][1]/(1 - trans[1][1])))
			print "Stationary distribution: ", (steady_emotion, 1 - steady_emotion)

#Calculate the Markov transitions on our own
message_types = ("Empathy", "GrowthMindset", "SuccessFailure", "Combined")
transitions = dict(Confidence = {message_type:[[0, 0], [0, 0]] for message_type in message_types},
				  Frustration = {message_type:[[0, 0], [0, 0]] for message_type in message_types})
for _,emotion_metrics in student_timeseries_emotion_metrics.items():
	#for each student, get their emotion metrics
	for emotion, metrics in emotion_metrics.items():
		#for each of the emotions, do time-series analysis
		prev_state = None
		for metric in metrics:
			amount = metric[emotion] #figure out what state they were in
			# if amount != 3: #ignore rows with neutral emotion reports
			state = int(amount < 3) #binarize their emotional state
			if prev_state is not None: #ignore the first row, because we don't have a prev state
				transitions[emotion]["Combined"][prev_state][state] += 1
				transitions[emotion][metric["Pedagogy"]][prev_state][state] += 1
			prev_state = state

print "Transition probabilities:"
convertTransitionCountsToLogProbabilities(transitions)

#Go through all the transition events and compute the likelihood
# of it being produced by either the null model or the alternate models
null_loglikelihood = 0
alt_loglikelihood = 0
for _,emotion_metrics in student_timeseries_emotion_metrics.items():
	#for each student, get their emotion metrics
	for emotion, metrics in emotion_metrics.items():
		#for each of the emotions, do time-series analysis
		prev_state = None
		for metric in metrics:
			amount = metric[emotion] #figure out what state they were in
			# if amount != 3: #ignore rows with neutral emotion reports
			state = int(amount < 3) #binarize their emotional state
			if prev_state is not None: #ignore the first row, because we don't have a prev state
				null_loglikelihood += math.log(transitions[emotion]["Combined"][prev_state][state])
				alt_loglikelihood += math.log(transitions[emotion][metric["Pedagogy"]][prev_state][state])
			prev_state = state

likelihood_ratio = 2 * (alt_loglikelihood - null_loglikelihood)
# 7 degrees of freedom difference because 2 parameters per model,
# and the alt is using an ensemble of 3 condition-specific models vs the generic
# Also, the condition-specific models have an extra implicit parameter
p = chi2.sf(likelihood_ratio, 7)

print "For the likelihood ratio test on our condition-based Markov models, we have p = %.3e" % p

print "\n----------\n"

print "Calculating Markov models for within-tutor emotion self-reports based on messages..."

transitions = dict(Confidence = {message_type:[[0, 0], [0, 0]] for message_type in message_types},
				  Frustration = {message_type:[[0, 0], [0, 0]] for message_type in message_types})
for _,emotion_metrics in student_timeseries_emotion_metrics.items():
	#for each student, get their emotion metrics
	for emotion, metrics in emotion_metrics.items():
		#for each of the emotions, do time-series analysis
		prev_state = None
		for metric in metrics:
			amount = metric[emotion] #figure out what state they were in
			# if amount != 3: #ignore rows with neutral emotion reports
			state = int(amount < 3) #binarize their emotional state
			if prev_state is not None: #ignore the first row, because we don't have a prev state
				transitions[emotion]["Combined"][prev_state][state] += 1
				for message_type in message_types[:3]:
					if metric[message_type + " Messages"] > 0:
						transitions[emotion][message_type][prev_state][state] += 1
			prev_state = state

print "Transition probabilities:"
convertTransitionCountsToLogProbabilities(transitions)

#Go through all the transition events and compute the likelihood
# of it being produced by either the null model or the alternate models
null_loglikelihoods = {emotion: {message_type: 0 for message_type in message_types[:3]} for emotion in ("Confidence", "Frustration")}
alt_loglikelihoods = {emotion: {message_type: 0 for message_type in message_types[:3]} for emotion in ("Confidence", "Frustration")}
for _,emotion_metrics in student_timeseries_emotion_metrics.items():
	#for each student, get their emotion metrics
	for emotion, metrics in emotion_metrics.items():
		#for each of the emotions, do time-series analysis
		prev_state = None
		for metric in metrics:
			amount = metric[emotion] #figure out what state they were in
			# if amount != 3: #ignore rows with neutral emotion reports
			state = int(amount < 3) #binarize their emotional state
			if prev_state is not None: #ignore the first row, because we don't have a prev state
				for message_type in message_types[:3]:
					if metric[message_type + " Messages"] > 0:
						null_loglikelihoods[emotion][message_type] += math.log(transitions[emotion]["Combined"][prev_state][state])
						alt_loglikelihoods[emotion][message_type] += math.log(transitions[emotion][message_type][prev_state][state])
			prev_state = state

for emotion, message_types in null_loglikelihoods.items():
	for message_type, null_loglikelihood in message_types.items():
		alt_loglikelihood = alt_loglikelihoods[emotion][message_type]
		likelihood_ratio = 2 * (alt_loglikelihood - null_loglikelihood)
		p = chi2.sf(likelihood_ratio, 1)
		print "For the likelihood ratio test on our Markov models for %s after receiving %s messages, we have p = %.3e" % (emotion, message_type, p)
				
print "Done."