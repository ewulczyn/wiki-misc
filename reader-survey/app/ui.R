library(shinythemes)
library(markdown)

# Define UI for application that draws a histogram
shinyUI(navbarPage("Wikimedia Reader Survey",
                   
                   # Sidebar with a slider input for the number of bins
                   
                   tabPanel("Tool", 
                            sidebarLayout(
                              
                              sidebarPanel(
                                
                                tags$div( HTML("<strong> Plot 1 </strong>")),
                                
                                selectInput("a11", label = "Information Need", 
                                            choices = c('any', 'fact','overview', 'in-depth', 'other'),
                                            selected = 'any'),
                                
                                
                                selectInput("a21", label = "Prior Knowledge", 
                                            choices = c('any', 'familiar', 'unfamiliar') ,               
                                            selected = 'any'),
                                
                                selectInput("a31", label = "Motivation", 
                                            choices = c('any', 'work/school', 'personal decision', 'current event', 'media', 'conversation', 'bored/curios/random', 'other'),                                            
                                            selected = 'any'), 
                                
                                tags$div( HTML("<strong> Plot 2 </strong>")),
                                
                                selectInput("a12", label = "Information_Need", 
                                            choices = c('any', 'fact','overview', 'in-depth', 'other'),
                                            selected = 'any'),
                                
                                
                                selectInput("a22", label = "Prior_Knowledge", 
                                            choices = c('any', 'familiar', 'unfamiliar') ,               
                                            selected = 'any'),
                                
                                selectInput("a32", label = "Motivation", 
                                            choices = c('any', 'work/school', 'personal decision', 'current event', 'media', 'conversation', 'bored/curios/random', 'other'),                                            
                                            selected = 'any'), 
                                width = 2
                                
                              ),
                              
                              # Show a plot of the generated distribution
                              
                              
                              mainPanel(
                                plotOutput("plot1"),
                                plotOutput("plot2")
                              )
                            )
                   ),
                   tabPanel("About",
                              includeMarkdown("about.md")
                   ),
                   theme = shinytheme("cosmo")
))