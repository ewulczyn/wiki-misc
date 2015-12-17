library(ggplot2)

d = fread('data/reader-survey-3-recoded.tsv', sep="\t", header=T)

d$Information_Need <- factor(d$Information_Need, levels= c('fact','overview', 'in-depth', 'other', 'no_response')) 
d$Prior_Knowledge <- factor(d$Prior_Knowledge, levels= c('familiar', 'unfamiliar', 'other', 'no_response')) 
d$Motivation <- factor(d$Motivation, levels= c('work/school', 'personal_decision', 'current_event', 'media', 'conversation', 'bored/curios/random', 'other', 'no_response')) 

shinyServer(function(input, output) {
  
  
  
  output$plot1 <- renderPlot({
    a11_value <-  input$a11
    a21_value <- input$a21
    a31_value <- input$a31
    
    d_curr = d
    
    if(a11_value != 'any'){
      d_curr = d_curr[d_curr[['Information_Need']] == a11_value]
    }
    
    if(a21_value != 'any'){
      d_curr = d_curr[d_curr[['Prior_Knowledge']] == a21_value]
    }
    
    if(a31_value != 'any'){
      d_curr = d_curr[d_curr[['Motivation']] == a31_value]
    }
    
    p1 <- ggplot(d_curr, aes(x=factor(Information_Need)))+
      geom_bar(stat="bin", width=0.7, fill="steelblue")+
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45, hjust = 1))+
      xlab("Information Need") 
    
    p2 <- ggplot(d_curr, aes(x=factor(Prior_Knowledge)))+
      geom_bar(stat="bin", width=0.7, fill="steelblue")+
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45, hjust = 1))+
      xlab("Prior Knowledge") 
    
    p3 <- ggplot(d_curr, aes(x=factor(Motivation)))+
      geom_bar(stat="bin", width=0.7, fill="steelblue")+
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45, hjust = 1))+
      xlab("Motivation") 
    
    multiplot(p1, p2, p3, cols=3)

    
    
  })
  
  output$plot2 <- renderPlot({
    a12_value <-  input$a12
    a22_value <- input$a22
    a32_value <- input$a32
    
    d_curr = d
    
    if(a12_value != 'any'){
      d_curr = d_curr[d_curr[['Information_Need']] == a12_value]
    }
    
    if(a22_value != 'any'){
      d_curr = d_curr[d_curr[['Prior_Knowledge']] == a22_value]
    }
    
    if(a32_value != 'any'){
      d_curr = d_curr[d_curr[['Motivation']] == a32_value]
    }
    
    p1 <- ggplot(d_curr, aes(x=factor(Information_Need)))+
      geom_bar(stat="bin", width=0.7, fill="steelblue")+
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      xlab("Information Need") 
    
    p2 <- ggplot(d_curr, aes(x=factor(Prior_Knowledge)))+
      geom_bar(stat="bin", width=0.7, fill="steelblue")+
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45, hjust = 1))+
      xlab("Prior Knowledge") 
  
    p3 <- ggplot(d_curr, aes(x=factor(Motivation)))+
      geom_bar(stat="bin", width=0.7, fill="steelblue")+
      theme_minimal()+
      theme(axis.text.x = element_text(angle = 45, hjust = 1))+
      xlab("Motivation") 
    
    multiplot(p1, p2, p3, cols=3)
    
    
    
  })
})