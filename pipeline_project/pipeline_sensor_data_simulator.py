import random, csv, datetime

'''Function to generate data for 1 cluster of sensors. It generates a list of 16 floats between 0 and 1'''

def valid_sensor_cluster_data():
    sensorData = []
    for i in range(16):
        sensorData.append(round(random.random(), 2))
    return [datetime.datetime.now()] + sensorData

'''Function to generate data with an errorfor 1 cluster of sensors. It generates a list whose last item is the sensor with the "Err"'''

def invalid_sensor_cluster_data():
    sensorData = []
    for i in range(16):
        sensorData.append(round(random.random(), 2))
    x = random.randint(0,15)
    sensorData[x] = "Err"
    sensor_data_with_error = sensorData[0:(x+1)]
    return  [datetime.datetime.now()] + sensor_data_with_error

'''Function to simulate data generation. If there is an error, the last sensor to be checked is the one with the error.'''

def pipeline_cluster_data():
    pipelineData = {}

    '''Randomize error generation in dataset. If x = 0, all sensors are ok'''

    x = random.randint(0,1)
    if x == 0:
        for i in range(1,33):
            pipelineData[i] = [i] + valid_sensor_cluster_data()

    if x == 1:
        
        '''Randomize selection of sensor cluster with error in dataset. a is the sensor cluster with the error'''

        a = random.randint(1,32)
        for i in range(1,a):
            pipelineData[i] = [i] + valid_sensor_cluster_data()

        pipelineData[a] = [a] + invalid_sensor_cluster_data()

    return pipelineData

'''Function to check if pipeline data has any errors.'''

def check_data(my_dict):

    '''For valid data all 512 sensors must return  floats else data is corrupt'''
    
    if len(my_dict) ==32 and len(my_dict[32]) == 18 and my_dict[32][17] != "Err":
        w= open("valid_data.csv", "a", newline='')
        writer = csv.writer(w)
        for key, val in my_dict.items():
            writer.writerow(val)
        message = "Pipeline status: \t OK \nData recorded in valid_data.csv"
        w.close()
     
    else:
        for key, val in my_dict.items():
            for i in range(0,len(val)):
                if val[i] == "Err":
                    sensor_number = i-1
                    error_code = "0" + str(key) + "0" + str(sensor_number)
                    message1 = ("Error {}".format(error_code))
                    message2 = ("Check cluster {} sensor {}".format(key, sensor_number))
                    
                    print(message1)
                    print(message2)
                    error_log ={"log": [val[1], message1,message2]}

                    '''Write generated error code to error log'''

                    w= open("pipeline_sensors _error_log.csv", "a", newline='')
                    writer = csv.writer(w)
                    for key_identifier, value in error_log.items():
                        writer.writerow(value) 
                    w.close()

                    '''Write corrupt data to invalid data csv file '''
                    
                    w= open("invalid_data.csv", "a", newline='')
                    writer = csv.writer(w)
                    for key, val in my_dict.items():
                        writer.writerow(val)
                    w.close()
                    message = "Pipeline status: \t DANGER!! \nData recorded in invalid_data.csv and pipeline_sensors _error_log.csv "

    return message

print (check_data(pipeline_cluster_data()))
