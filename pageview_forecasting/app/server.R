library(shiny)
library(forecast)
library(ggplot2)
library(scales)
library(data.table)

d = fread('data/cube.csv', sep=",", header=T)
d = d[2:(nrow(d)-1), ]
dates = seq(as.Date(d$YearMonth[1]), length = (nrow(d) + 12), by = "month")
d[['YearMonth']] = NULL
d <- sapply(d, as.numeric)

shinyServer(function(input, output) {
  
  predictions = NULL
  past = NULL
  
  output$forecast_plot <- renderPlot({
    
    months <-  input$months
    method <- input$method
    
    
    
    if (input$access == 'desktop + mobile'){
      y_name_1 = paste(input$project,'desktop', input$country, sep='/')
      y_name_2 = paste(input$project,'mobile web', input$country, sep='/')
      y = d[,y_name_1] + d[,y_name_2]
      
    }else{
      y_name = paste(input$project,input$access, input$country, sep='/')
      y = d[,y_name]
    }
    
    
    dlm = data.frame(x = dates[1:nrow(d)] ,  y = y)
    past <<- dlm
    
    
    if(method == 'arima'){
      fit <- auto.arima(y)
      pred = forecast(fit, months)
      fitted_values = fitted(fit)
      dlm$fitted = fitted_values
      pred = data.frame(pred)
      pred$YearMonth = dates[(nrow(d)+1):(nrow(d)+months)]
      
      p <- ggplot(pred, aes(x=YearMonth, y=Point.Forecast)) +
        geom_ribbon(aes(ymin=Lo.95, ymax=Hi.95), alpha=0.1) + geom_line() + 
        #geom_line(data= dlm, aes(x=x, y = fitted ), colour="orange") +
        geom_line(data= pred, aes(x = YearMonth, y = Point.Forecast ), colour="orange") +
        geom_line(data= dlm, aes(x=x, y = y)) +
        geom_point(data= dlm, aes(x=x, y = y)) +
        xlab("") +
        ylab("Pageviews per Month") +
        expand_limits(y=0)
      print(p)
      pred = pred[, c('YearMonth', 'Point.Forecast', 'Lo.95', 'Hi.95')]  
      colnames(pred) <- c('YearMonth', 'fit', 'lwr', 'upr')
      predictions <<- pred
    }else{
      p <- ggplot(dlm,aes(x=x,y=y)) + 
        geom_point() +
        stat_smooth(method="lm",fullrange=TRUE, colour = 'orange')+
        scale_x_date(breaks = "3 month",  labels=date_format("%b-%Y"), limits = c(dates[1], dates[(nrow(d) + months)]) ) +
        xlab("") +
        ylab("Pageviews per Month") +
        expand_limits(y=0)
      
      fit = lm(y ~ x, dlm) 
      newdata = data.frame(x = dates[(nrow(d)+1):(nrow(d)+months)])
      pred = predict(fit, newdata, interval = "confidence")
      pred =  data.frame(pred)
      pred$YearMonth = dates[(nrow(d)+1):(nrow(d)+months)]
      pred = pred[, c('YearMonth', 'fit', 'lwr', 'upr')]  
      
      predictions <<- pred
      print(p)
    }
    
  })
  
  output$download_forecast <- downloadHandler(
    filename = function() {
      paste(input$method, input$project,input$access, input$country, 'forecast.csv', sep='.')
    },
    content = function(file){
      write.table(predictions, file, row.names = FALSE, sep = ",", quote = TRUE)
    }
  )
  
  output$download_past <- downloadHandler(
    filename = function() {
      paste(input$project, input$access, input$country, 'past.csv', sep='.')
    },
    content = function(file){
      write.table(past, file, row.names = FALSE, sep = ",", quote = TRUE)
    }
  )
  
  
  output$forecast_table <- renderDataTable(
{
  print(paste(input$months, input$method,input$access, input$country, input$project ))
  data.frame(predictions)
},
options = list(paging = FALSE, searching = FALSE)
  )

})