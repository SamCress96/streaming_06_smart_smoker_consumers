# streaming-05-smart-smoker
# Steps taken to create smart-smoker
1. Define a function to clear queues when file has been run previously. 
2. Define a function to only show rabbitmq pop up when set to true. 
3. Set host and set queue names. 3 queues are needed for the smart-smoker to work. 
4. Define a function to create and send a message to the appropriate queue each execution. 
5. Create connection and channel within the rabbitmq server, also declare queue name and create durable queue. 
6. Define function to read from smart-smoker CSV and create message.
7. Use fstrings to create message from the data, first fstring pulls the timestamp from column one. 
8. Create 3 seperate messages that send the timestamp followed by either the smoker temp, food 1 temp, or food 2 temp. 
9. Set the producer to sleep for 30 seconds. 
10. Use the python idiom to call appropriate functions. 
# streaming-06-smart-smoker-consumers
1. Create three different consumers that will each read from 3 different queues.
2. Consumer one will recieve messages from the smart_smoker queue. 
3. Messages will consist of smoker temps.
4. If the temperature of the smoker decreases by 15 or more degrees in 2.5 minutes a smoker alert will be triggered. 
5. To create smoker alert:
    # Create a callback function with the following
    # Create a deque to hold 5 messages
    # Calculate change in temperature by subracting the newest message (temp) in the deque from the oldest message (temp) in the deque. 
    # Set a smoker alert threshold of 15 and create an if statement that if the change in temp is greater than or equal to 15 then print smoker alert. 
6. To create food1 and food2 consumers:
7. Create a callback function like the one created for the smoker consumer. 
8. Create a deque to hold 20 messages, since we are setting an alert for when the food temp has not increased by at least one degree in 10 minutes. At one reading every 1/2 minute, the food deque max length is 20 (10 min * 1 reading/0.5 min) 
10. To create food stall alert:
    # Calculate change in temperature by subracting last message in the deque (message 20) from the first message in the deque. 
    # Create a food stall threshold of 1 degree. 
    # Create an if statement: if change in food temp is less than 1 degree print food stall!
11. Change temperature to floats and split messages when needed (this will be to seperate timestamp from temperature.)
12. Define a main function to create channel connection 
13. Make sure to sent auto_ack to False or you may get an unknown delivery tag error (I got this error at first)
14. Call main function using python idiom
15. Ensure correct queues are being referenced in the correct consumers.
16. Run the BBQ producer and then run the BBQ producers and wait for alerts!!