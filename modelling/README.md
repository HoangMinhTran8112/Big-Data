# Modelling Code Guide

This folder contains the modelling code for **London Bike Safety Prediction**.
It covers **data ingestion from MongoDB**, **feature engineering**, **training and evaluation with tree-based ensembles**, and **experiment tracking with MLflow**.

---

## 1. MLflow Setup

We setup **MLflow** to track experiments (parameters, metrics, and artifacts) and store it permanently.

### Step 1. Create or activate Conda environment

```bash
conda create -n bdg_modelling python=3.11 -y
conda activate bdg_modelling
```

### Step 2. Install required packages

```bash
pip install mlflow sqlalchemy
```

### Step 3. Start MLflow tracking server

Navigate to the `modelling/` folder and run:

```bash
mlflow server \
  --backend-store-uri sqlite:///mlruns.db \
  --default-artifact-root ./mlruns \
  --host 127.0.0.1 \
  --port 5000 \
  --workers 1
```

MLflow UI will be available at: [http://127.0.0.1:5000](http://127.0.0.1:5000)

---

## 2. Notebook Environment Setup

The modelling notebooks require careful setup on **PySpark**.
Follow these steps:

### Step 1. Install Java

Download and install **JDK 17**.
Set environment variables:

  ```bash
  JAVA_HOME="C:\Program Files\Java\jdk-17"
  PATH="%JAVA_HOME%\bin;%PATH%"
  ```

### Step 2. Install PySpark 3.5.6

Download from [Apache Spark](https://spark.apache.org/downloads.html)

Make sure to match Hadoop version and set environment variables:

```bash
SPARK_HOME=/path/to/spark-3.5.6
PATH=$SPARK_HOME/bin:$PATH
```

### Step 3. Connect to Anaconda Environment

In your preferred IDE (e.g., Jupyter, VSCode, PyCharm):

* Select the **bdg\_modelling** environment.
* Install additional packages:

  ```bash
  pip install pymongo[srv]==3.11 pyspark==3.5.6 pandas numpy matplotlib seaborn \
              scipy scikit-learn imbalanced-learn openai mlflow sqlalchemy
  ```

### Step 4. Run the Notebook

* Open the modelling notebook (`assignment3_modelling.ipynb` or equivalent).
* Execute cells sequentially from top to bottom.