pipeline {
  parameters {
    choice(name: "HOST", choices: ['aws-virginia', 'other'], description: "Choose host")
    string(name: "BRANCH", defaultValue: "", description: "Choose branch")
    string(name: "USER", defaultValue: "", description: "Username")
    string(name: "PASSWORD", defaultValue: "", description: "Password")
  }

  environment {
    CREDENTIALS = credentials("$USER")
    DIR_NAME = "${JOB_NAME}_${BRANCH}"
  }

  agent {label "$HOST"}

  stages {
    stage("Authorization") {
      when {
        allOf {
          expression { "$PASSWORD" == "%{CREDENTIALS_PSW}" && "$USER" == "${CREDENTIALS_USR}" }
        }
      }
      steps {
        echo "Hello World"
      }
    }

    stage("Preparing workspace") {
      steps {
        sh '''
        sudo su &&
        ./opt/"$JOB_NAME"/db_ex_connections/stop_all.sh
        cd /opt &&
        rm -rf "$DIR_NAME" &&
        cp -r /home/ubuntu/workspace/"$DIR_NAME" /opt/"$JOB_NAME" &&
        '''
      }
    }

    stage("Running workspace") {
      steps {
        sh '''
        sudo su &&
        ./opt/"$JOB_NAME"/db_ex_connections/start_all.sh
        '''
      }
    }










  }
}




