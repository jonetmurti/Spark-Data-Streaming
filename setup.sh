FILE=/home/project2-bigdata/Frameworks/spark-2.4.7-bin-hadoop2.7.tgz
if [ ! -f "$FILE" ]; then
    echo "Downloading $FILE."
    wget -P /home/project2-bigdata/Frameworks https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
else 
	echo "$FILE already exist."
fi

KAFKA_FILE=/home/project2-bigdata/Frameworks/kafka_2.13-3.1.0.tgz
if [ ! -f "$KAFKA_FILE" ]; then
    echo "Downloading $KAFKA_FILE."
	wget -P /home/project2-bigdata/Frameworks https://dlcdn.apache.org/kafka/3.1.0/kafka_2.13-3.1.0.tgz
else 
	echo "$KAFKA_FILE already exist."
fi

if [ ! -d "/home/project2-bigdata/Frameworks/spark-2.4.7-bin-hadoop2.7" ]; then
	echo "Extracting $FILE."
	tar xvf $FILE
else
	echo "$FILE already extracted."
fi

if [ ! -d "/home/project2-bigdata/Frameworks/kafka_2.13-3.1.0" ]; then
	echo "Extracting $KAFKA_FILE."
	tar xvf $KAFKA_FILE
	echo "export KAFKA_HOME=/home/project2-bigdata/Frameworks/kafka_2.13-3.1.0" >> ~/.bashrc
	echo "export PATH=$PATH:$KAFKA_HOME/bin" >> ~/.bashrc
	sudo touch /etc/systemd/system/zookeeper.service
	sudo cat ./zookeeper.service > /etc/systemd/system/zookeeper.service
	sudo bash -c 'cat ./zookeeper.service > /etc/systemd/system/zookeeper.service'
	sudo touch  /etc/systemd/system/kafka.service
	sudo bash -c 'cat ./kafka.service > /etc/systemd/system/kafka.service'
	sudo systemctl start kafka
	sudo journalctl -u kafka
else
	echo "$KAFKA_FILE already extracted."
fi

if [ ! -d "./venv" ]; then
	echo "Creating virtual environment"
	python3 -m venv venv
else
	echo "Virtual environment already created."
fi

source ./venv/bin/activate

PYTHON_VERSION=$(which python3)
if [ $PYTHON_VERSION != "/usr/bin/python3\n" ]; then
	pip install -r ./requirements.txt
	rm -f ./venv.tar.gz
	venv-pack -o venv.tar.gz
else 
	echo "Environment not set."
fi