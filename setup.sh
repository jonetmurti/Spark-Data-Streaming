FILE=/home/project2-bigdata/Frameworks/spark-2.4.7-bin-hadoop2.7.tgz
if [ ! -f "$FILE" ]; then
    echo "Downloading $FILE."
    wget -P /home/project2-bigdata/Frameworks https://archive.apache.org/dist/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
else 
	echo "$FILE already exist."
fi

if [ ! -d "/home/project2-bigdata/Frameworks/spark-2.4.7-bin-hadoop2.7" ]; then
	echo "Extracting $FILE."
	tar xvf $FILE
else
	echo "$FILE already extracted."
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
	rm ./venv.tar.gz
	venv-pack -o venv.tar.gz
else 
	echo "Environment not set."
fi