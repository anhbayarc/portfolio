q5 <- read.csv("Final/US Population.csv")
Time <- q5$TIME
Year <- q5$YEAR
Population <- gsub(",", "",q5$POPULATION)
Population <- as.numeric(Population)

#a
q5model <- lm(Population~Time)
summary(q5model)
#estimated regression model is = Population = 108961085 + 2311504*Time
#99% percentage of the variation in Y is explained by regression
#b
#H0 : No Autocorrelation exists
#H1:  Autocorrelation exists
#Informal test of autocorrelation
resid_q5a <- residuals(q5model)
q5_timeperiods <- 0:69
length(resid_q5a)
plot(x=q5_timeperiods, type="b", y=resid_q5a, pch=19, 
     xlab = "Time", ylab = "Residuals", 
     main = "Time-Sequence Plot")
abline(h=0)

lag.plot(resid_q5a, lags = 1, do.lines = FALSE, 
         diag = FALSE, 
         main = "Residuals versus Lag 1 Residuals")
abline(h=0, v=0)
#Formal test of autocorrelation. Durbin Watson d test
lmtest::dwtest(q5model)
# p value is very small so existence of autocorrelation Dw value is close to the 0 so its more positively correlated. So in our model autocorrelation exists.So Reject H0.

#c creating lagged value of dependent variable to the equation
lag_population <- Hmisc::Lag(Population,1)
q5lagmodel <- lm(Population~Time+lag_population)
summary(q5lagmodel)
# result from the regression = Population = 1.122 + 2.272*Time+0.9092*Population(t-1)

#d added lag variable so Durbin-Watson d test is no longer appropriate

residq5d <- residuals(q5lagmodel)

timeperiods <- 1:(NROW(residq5d))
plot(x=timeperiods, type="b", y=residq5d, pch=19, 
     xlab = "Time", ylab = "Residuals", main = "Time-Sequence Plot")
abline(h=0)
#From the informal test there is autocorrelation

dwstatq5 <- lmtest::dwtest(q5lagmodel)

### Detect using Runs Test -- indicates randomness
randtests::runs.test(residq5d, alternative = "two.sided")
library(ecm)

durbinH(q5lagmodel, "lag_population")

qnorm(0.05, mean = 0, sd = 1, lower.tail = FALSE)

#Since 7.30622 > 1.644 that means autocoreelation is not corrected.


#e creating model with two period lagged
lag_population2 <- Hmisc::Lag(Population,2)
q5lagmodel2 <- lm(Population~Time+lag_population+lag_population2)
summary(q5lagmodel2)
#model is Population = 3.958 + 8.1150*Time + 1.769*Population(t-1) - 0.8027*Population(t-2)

#f
residq5d2 <- residuals(q5lagmodel2)

timeperiods <- 1:(NROW(residq5d2))
plot(x=timeperiods, type="b", y=residq5d2, pch=19, 
     xlab = "Time", ylab = "Residuals", main = "Time-Sequence Plot")
abline(h=0)

dwstatq5_2 <- lmtest::dwtest(q5lagmodel2)
#Since its a high level lagged model so I'm using the Breusch-Godfrey for test for autocorrelation. 
lmtest::bgtest(q5lagmodel2, order = 2)
#H0: No evidence of autocorrelation
#H1: Autocorrealtion exists

#Since the p-value is more than 0.05 we fail to reject H0. No autocorrelation detected in the model

#g
#predict Year 2000
predict(q5lagmodel2, data.frame(Time = 71, lag_population = 272690813, lag_population2 = 270248003))

#Predict Year 2001
predict(q5lagmodel2, data.frame(Time = 72, lag_population = 275123641, lag_population2 = 272690813))