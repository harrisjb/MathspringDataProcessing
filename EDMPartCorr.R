data = read.table("<link to data>", sep=',', header = TRUE)
#summary(data)
#plot(data$Pedagogy, data$posttest.Score)

#variables measured in pre and post
var <- c('interest','excitement','confusion','frustration','perforamnceAvoidance','perforamnceApproach',
         'learningOriented','mathValue','mathLiking','PostTestScore','normalizedLearningGain', 'correctness',
         'QuestionDifficult','numProblemsSeen','numMistakes' , 'numHints', 'learningEstimation')

#output dataframes
op_df = matrix(NA, nrow = 17, ncol = 7)

for (i in 1:17){
  op_df[i,1] = var[i] 
}
#m -> pedagogy or messages
for (m in c(4,5,6)){
  #k -> variables pre
  k <- 11
  #column for output
  if (m == 4) 
      c = 2
  else if (m == 5)
      c = 4
  else
     c = 6
  for (i in seq(0, 18, 2)){
      msg <- data[,m]
      total <- data$Total.Messages
      time <- data$TimeInTutor
      total <- total/time
      diff <- data$AvgProblemDifficulty
      pre <- data[,k+i]
      post <- data[,k+i+1]
      
      #ignore "I dont know"
      for (j in 1:62){
        if(is.na(data[j,k+i])){
          pre <- pre[-j]
          post<- post[-j]
          msg <- msg[-j]
          total <- total[-j]
          time <- time[-j]
          diff <- diff[-j]
        }
      }
      #ignore "I dont know"
      for (j in 1:62){
        if(is.na(data[j,k+i+1])){
          pre <- pre[-j]
          post<- post[-j]
          msg <- msg[-j]
          total <- total[-j]
          time <- time[-j]
          diff <- diff[-j]
        }
      }
      
      #cor(msg,post | control time,pre,total)
      xres <- residuals(lm(msg ~  pre  + time + total , na.action=na.exclude))
      yres <- residuals(lm(post ~ pre  + time + total  , na.action=na.exclude))
      
      #test on cor(xres,yres)
      op_df[i/2+1,c] = cor.test(xres, yres)$estimate #partial correlation
      op_df[i/2+1,c+1] = cor.test(xres, yres)$p.value #p value

    }
}

for (m in c(4,5,6)){ 
  
  if (m == 4) 
    c = 2
  else if (m == 5)
    c = 4
  else
    c = 6
  
  msg <- data[,m]
  total <- data$Total.Messages
  time <- data$TimeInTutor
  total <- total/time
  
  #nlg
  nlg <- data$NormalizedLearningGain
  xres <- residuals(lm(msg ~  time + total, na.action=na.exclude))
  yres <- residuals(lm(nlg ~ time + total, na.action=na.exclude))
  op_df[11,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[11,c+1] = cor.test(xres, yres)$p.value #p value
  
  #correctness
  correct <- data$NumIncorrect / (data$NumCorrect + data$NumIncorrect)
  xres <- residuals(lm(msg ~  time + total, na.action=na.exclude))
  yres <- residuals(lm(correct ~ time + total, na.action=na.exclude))
  op_df[12,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[12,c+1] = cor.test(xres, yres)$p.value #p value
  
  #difficulty
  diff <- data$AvgProblemDifficulty
  xres <- residuals(lm(msg ~  time + total, na.action=na.exclude))
  yres <- residuals(lm(diff ~ time + total, na.action=na.exclude))
  op_df[13,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[13,c+1] = cor.test(xres, yres)$p.value #p value
  
  #probSeen
  probSeen <- data$NumCorrect + data$NumIncorrect
  xres <- residuals(lm(msg ~  time + total, na.action=na.exclude))
  yres <- residuals(lm(probSeen ~ time + total, na.action=na.exclude))
  op_df[14,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[14,c+1] = cor.test(xres, yres)$p.value #p value
  
  #mistakesMade
  mist <- data$NumMistakes
  xres <- residuals(lm(msg ~  time + total, na.action=na.exclude))
  yres <- residuals(lm(mist ~ time + total, na.action=na.exclude))
  op_df[15,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[15,c+1] = cor.test(xres, yres)$p.value #p value
  
  #hints
  hints <- data$NumHints / (data$NumCorrect + data$NumIncorrect)
  xres <- residuals(lm(msg ~  time + total, na.action=na.exclude))
  yres <- residuals(lm(hints ~ time + total, na.action=na.exclude))
  op_df[16,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[16,c+1] = cor.test(xres, yres)$p.value #p value
  
  #learningEst
  #ignore "na
  learn <- data$LearningEstimation
  for (j in 1:61){
    if(is.na(data$LearningEstimation[j])){
      learn <- learn[-j]
      msg <- msg[-j]
      total <- total[-j]
      time <- time[-j]
      diff <- diff[-j]
    }
  }
  xres <- residuals(lm(msg ~  time+total , na.action=na.exclude))
  yres <- residuals(lm(learn ~ time+total , na.action=na.exclude))
  op_df[17,c] = cor.test(xres, yres)$estimate #partial correlation
  op_df[17,c+1] = cor.test(xres, yres)$p.value #p value
}



op_df_top = matrix(NA, nrow = 1, ncol = 7)
op_df_top[1,1] = "Variable"
op_df_top[1,2] = "EmpathyPartialCorrelation"
op_df_top[1,3] = "p value"
op_df_top[1,4] = "GrowthPartialCorrelation"
op_df_top[1,5] = "p value"
op_df_top[1,6] = "SuccessFailurePartialCorrelation"
op_df_top[1,7] = "p value"

print(op_df[17,]) 
op = rbind(op_df_top,op_df)

write.table(op, file = "<path to output>/PartialCorrAll.csv", sep = ',', col.names = FALSE)
