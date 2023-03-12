Question  1:  Please select the statements that are correct 
KIND OF: Kafka Node is responsible to store topics
YES: Zookeeper is removed form Kafka cluster starting from version 4.0
YES: Retention configuration ensures the messages not get lost over specific period of time.
YES: Group-Id ensures the messages are distributed to associated consumers

Question 2: Please select the Kafka concepts that support reliability and availability
YES: Topic Replication
NO: Topic Paritioning
NO: Consumer Group Id
YES: Ack All

Question 3: Please select the Kafka concepts that support scaling *
NO: Topic Replication
YES: Topic Partitioning
YES: Consumer Group Id
NO: Ack All

Question 4: Please select the attributes that are good candidates for partitioning key. Consider cardinality of the field you have selected and scaling aspects of your application
YES:payment_type
YES:vendor_id
passenger_count
total_amount
tpep_pickup_datetime
tpep_dropoff_datetime

Question 5:  Which configurations below should be provided for Kafka Consumer but not needed for Kafka Producer *
YES: Deserializer Configuration
YE:Topics Subscription
Bootstrap Server
YES:Group-Id
YES: Offset
NO: Cluster Key and Cluster-Secret


