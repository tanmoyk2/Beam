import os
import time
from google.cloud import pubsub_v1
import datetime
import filedateappend



if __name__=="__main__":
  project='beam-290211'
  pusub_topic='projects/beam-290211/topics/topic1'
  path_survice_account='[Give the path of the credential file]' 
  os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=path_survice_account
  input_file='C:/googleCloud/beam/pubsub/storedata.csv'
  final_file='C:/googleCloud/beam/pubsub/streamdata.csv'
  publisher=pubsub_v1.PublisherClient()
  try:
    os.remove(final_file)
    print('Removing existing straming file')
  except FileNotFoundError:
    print('File is not present')
 
  print('-----Create a file with current timestamp----')
  filedateappend.CreateNewFile(input_file,final_file)
  print('-----File {} creation complete----'.format(final_file))

  
  with open(final_file,'rb') as ifp:
    header=ifp.readline()

    for line in ifp:
      event_data=line
      print('publishing {0} to {1}'.format(event_data,pusub_topic))
      publisher.publish(pusub_topic,event_data)
      time.sleep(2)


