library(shiny)
library(shinythemes)
library(data.table)

# Derive set of options from header
d = fread('data/cube.csv', sep=",", header=T)
d[['month']] = NULL
columns = colnames(d)
access_methods = c()
projects = c()
countries = c()

for(c in columns) {
  elems = strsplit(c, '/')
  projects = append(projects, elems[[1]][1])
  access_methods = append(access_methods, elems[[1]][2])
  countries = append(countries, elems[[1]][3])
}

access_methods = append(access_methods, 'desktop + mobile')



# Define UI for application that draws a histogram
shinyUI(fluidPage(
  
  theme = shinytheme("cosmo"),
  
  # Application title
  titlePanel("Pageview Forecasting"),
  
  # Sidebar with a slider input for the number of bins
  sidebarLayout(
    
    sidebarPanel(
      
      radioButtons("method", "Forcasting Method:",
                   c("Linear" = "linear", "ARIMA" = "arima")),
      
      sliderInput("months",
                  "Number of Months:",
                  min = 1,
                  max = 12,
                  value = 6),
      
      selectInput("access", label = "Access", 
                  choices = unique(access_methods),
                  selected = 'desktop'),
      
      
      selectInput("country", label = "Country", 
                  choices = unique(countries) ,               
                  selected = 'United States'),
      
      selectInput("project", label = "Project", 
                  choices = unique(projects),                                            
                  selected = 'en.wikipedia'), 
      
      downloadButton("download_forecast", "Download Forecast"),
      
      downloadButton("download_past", "Download Past")
        
    ),
    
    # Show a plot of the generated distribution
    mainPanel(
      plotOutput("forecast_plot"), 
      dataTableOutput("forecast_table")
    )
  )
))