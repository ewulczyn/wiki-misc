library(shinythemes)

# Derive set of options from header
d = fread('data/cube.csv', sep=",", header=T)
d[['month']] = NULL
columns = colnames(d)
access_methods = c()
projects = c()
countries = c()

for(c in columns) {
  elems = strsplit(c, '/')[[1]]
  if (length(elems) == 3) {
    projects = append(projects, elems[1])
    access_methods = append(access_methods, elems[2])
    countries = append(countries, elems[3])
  }
}

access_methods = append(access_methods, 'desktop + mobile')
countries = sort(countries)
projects = sort(projects)



# Define UI for application that draws a histogram
shinyUI(fluidPage(
  
  theme = shinytheme("cosmo"),
  
  # Application title
  titlePanel("Wikipedia Pageview Forecasting"),
  
  # Sidebar with a slider input for the number of bins
  sidebarLayout(
    
    sidebarPanel(
      
      radioButtons("method", "Forecasting Method:",
                   c( "ARIMA" = "arima", "Linear Regression" = "linear")),
      
      sliderInput("months",
                  "Number of Months:",
                  min = 1,
                  max = 36,
                  value = 12),
      
      selectInput("access", label = "Access Method", 
                  choices = unique(access_methods),
                  selected = 'desktop + mobile'),
      
      
      selectInput("country", label = "Country", 
                  choices = unique(countries) ,               
                  selected = 'United States'),
      
      selectInput("project", label = "Project", 
                  choices = unique(projects),                                            
                  selected = 'en.wikipedia'), 
      
      downloadButton("download_forecast", "Download Forecast"),
      
      downloadButton("download_past", "Download Past"),
      
      tags$div( HTML("<strong> About this data</strong>")),
      
      tags$div(
          HTML('Data for this application comes from the <a href = "https://wikitech.wikimedia.org/wiki/Analytics/Data/Projectview_hourly"> projectview_hourly table</a> maintained by the Wikimedia analytics team.' )
      )
        
    ),
    
    # Show a plot of the generated distribution
    mainPanel(
      plotOutput("forecast_plot"), 
      dataTableOutput("forecast_table")
    )
  )
))