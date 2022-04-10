pipeline {
  parameters {
    choice(name: "HOST", choices: ['aws-virginia', 'other'], description: "Choose host")
    string(name: "BRANCH", defaultValue: "", description: "Choose branch")
    string(name: "USER", defaultValue: "", description: "Username")
    string(name: "PASSWORD", defaultValue: "", description: "Password")
  }

  environment {
    CREDENTIALS = credentials("$USER")
    DIR_NAME = "hft_${BRANCH}"
  }

  agent {label "$HOST"}

  stages {


    stage("Authorization") {
      when {
          equals(actual: "$PASSWORD", expected: "${CREDENTIALS_PSW}")
      }
      steps {
        echo "Authorized"
      }
    }

    stage("Preparing workspace") {
      steps {
        sh '''
        sudo su &&
        chmod +x /opt/hft/db_ex_connections/stop_all.sh &&
        cd /opt &&
        bash /opt/hft/db_ex_connections/stop_all.sh &&
        cp -r /home/ubuntu/workspace/"$DIR_NAME"/* /opt/hft &&
        rm /opt/hft/Jenkinsfile
        rm /opt/hft/Dockerfile
        '''
      }
    }

    stage("Running workspace") {
      steps {
        sh '''
        sudo su &&
        cd /opt &&
        chmod +x /opt/hft/db_ex_connections/start_all.sh &&
        bash /opt/hft/db_ex_connections/start_all.sh
        '''
      }
    }
  }
}




