# install Java
echo "installing java-21-amazon-corretto-devel ..." >> setup.log
sudo yum install -y -v java-21-amazon-corretto-devel >> setup.log

echo "\ncopying cacerts to kafka_truststore.jks ..." >> setup.log
cp /usr/lib/jvm/java-21-amazon-corretto.x86_64/lib/security/cacerts kafka_truststore.jks

# Couchbase connector
echo "\ndownloading couchbase-kafka-connect-couchbase-4.2.8.zip ..." >> setup.log
wget https://packages.couchbase.com/clients/kafka/4.2.8/couchbase-kafka-connect-couchbase-4.2.8.zip

echo "\ncopying couchbase-kafka-connect-couchbase-4.2.8.zip to s3://$1 ..." >> setup.log
aws s3 cp couchbase-kafka-connect-couchbase-4.2.8.zip s3://$1

# Amazon DocumentDB connector
echo "\ncreate directories for Amazon DocumentDB custom plugin ..." >> setup.log
cd /home/ec2-user
mkdir -p docdb-custom-plugin
mkdir -p docdb-custom-plugin/mongo-connector
mkdir -p docdb-custom-plugin/msk-config-providers

echo "\ndownloading mongo-kafka-connect-1.15.0-all.jar ..." >> setup.log
cd /home/ec2-user/docdb-custom-plugin/mongo-connector
wget https://repo1.maven.org/maven2/org/mongodb/kafka/mongo-kafka-connect/1.15.0/mongo-kafka-connect-1.15.0-all.jar

echo "\ndownloading msk-config-providers-0.3.1-with-dependencies.zip ..." >> /home/ec2-user/setup.log
cd /home/ec2-user/docdb-custom-plugin/msk-config-providers
wget https://github.com/aws-samples/msk-config-providers/releases/download/r0.3.1/msk-config-providers-0.3.1-with-dependencies.zip

echo "\nunzipping msk-config-providers-0.3.1-with-dependencies.zip ..." >> /home/ec2-user/setup.log
unzip msk-config-providers-0.3.1-with-dependencies.zip

echo "\ndeleting msk-config-providers-0.3.1-with-dependencies.zip ..." >> /home/ec2-user/setup.log
rm msk-config-providers-0.3.1-with-dependencies.zip

echo "\ncreating docdb-custom-plugin.zip ..." >> /home/ec2-user/setup.log
cd /home/ec2-user
zip -r docdb-custom-plugin.zip docdb-custom-plugin

echo "\ncopying docdb-custom-plugin.zip to s3://$1 ..." >> setup.log
aws s3 cp docdb-custom-plugin.zip s3://$1

# Kafka
echo "\ndownloading kafka_2.13-4.0.0.tgz ..." >> setup.log
wget https://dlcdn.apache.org/kafka/4.0.0/kafka_2.13-4.0.0.tgz

echo "\nextracting kafka_2.13-4.0.0.tgz ..." >> setup.log
tar -xzf kafka_2.13-4.0.0.tgz

# AWS MSK IAM auth
echo "\ndownloading aws-msk-iam-auth-2.3.2-all.jar ..." >> setup.log
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v2.3.2/aws-msk-iam-auth-2.3.2-all.jar

echo "\ncopying aws-msk-iam-auth-2.3.2-all.jar to kafka_2.13-4.0.0/libs/. ..." >> setup.log
cp aws-msk-iam-auth-2.3.2-all.jar kafka_2.13-4.0.0/libs/.

# Mongo shell
echo "\ninstalling mongodb-mongosh-shared-openssl3 ..." >> setup.log
echo -e "[mongodb-org-5.0] \nname=MongoDB Repository\nbaseurl=https://repo.mongodb.org/yum/amazon/2023/mongodb-org/5.0/x86_64/\ngpgcheck=1 \nenabled=1 \ngpgkey=https://pgp.mongodb.com/server-5.0.asc" | sudo tee /etc/yum.repos.d/mongodb-org-5.0.repo
sudo yum install -y -v mongodb-mongosh-shared-openssl3

# create Amazon DocumentDB trust store
echo "\nexecuting createTruststore.sh ..." >> setup.log
./createTruststore.sh

echo "\ncopying docdb-truststore.jks to s3://$1 ..." >> setup.log
aws s3 cp /home/ec2-user/docdb-truststore.jks s3://$1

# create Kafka client properties file
echo "\ncreating /home/ec2-user/kafka_2.13-4.0.0/config/client.properties ..." >> setup.log
echo "ssl.truststore.location=/home/ec2-user/kafka_truststore.jks" >> kafka_2.13-4.0.0/config/client.properties
echo "security.protocol=SASL_SSL" >> kafka_2.13-4.0.0/config/client.properties
echo "sasl.mechanism=AWS_MSK_IAM " >> kafka_2.13-4.0.0/config/client.properties
echo "sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;" >> kafka_2.13-4.0.0/config/client.properties
echo "sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler" >> kafka_2.13-4.0.0/config/client.properties

# setup complete
echo "\nsetup complete ..." >> setup.log
