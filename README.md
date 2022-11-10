# TwitterInsights

Twitter insights pulls data from the publicly available Twitter api.
The program counts the number of hashtags a user tweeted within some duration of time then prints out the results to the console and save them to a partitioned files.

How to:

Populate the properties in the file named application.properties.template under resources folder

    Run 'mvn clean install' to resolve the dependencies 
    create folder called 'results' under main folders
    Click run on intelliJ IDE and stop to terminate program

Command line options:

    mvn clean package
    java -jar  jar-file-name.jar
    Note: you might need to make the jar include dependencies and be executable for this option.   
