# **hft_algo - Complex data collection and analyzing project**

## **main purpose**

Asynchronous functions download data from cryptocurrency exchanges using REST API and Websocket methods.
All of this data is automatically stored in Postgresql database hosted on the same remote server.

### **key directory**

***db_ex_connections** --> directory where the data collectors are divied by exchanges.
For each exchange running two scripts saving orderbooks and last trades.
All parameters are saved into ***exchanges.txt*** file and data collectors map values from this file.
The output of each application is a .log file allowing to control the applications

### **automation scripts**

All infrastructure is easily managable with Bash scripts that allow to start/stop each collector, each exchange or whole data collection scripts at once.

![start](https://user-images.githubusercontent.com/89335034/164598448-e08ce0d8-9cd9-4fb6-ad68-325ebb389be8.png)


### **CI/CD with Jenkins + Docker**

Whole application is a Docker image which is build and deploy with Jenkins multibranch pipeline.
