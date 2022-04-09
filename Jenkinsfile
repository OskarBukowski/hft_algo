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
        sh '$(whoami) > t.txt'
      }
    }

    stage("Preparing workspace") {
      steps {
      /* ./opt/"$JOB_NAME"/db_ex_connections/stop_all.sh */
        sh '''
        sudo su &&
        cd /opt &&
        rm -rf "$DIR_NAME" &&
        cp -r /home/ubuntu/workspace/"$DIR_NAME" /opt/"$JOB_NAME"
        '''
      }
    }

    /*
    stage("Running workspace") {
      steps {
        sh '''
        sudo su &&
        ./opt/"$JOB_NAME"/db_ex_connections/start_all.sh
        '''
      }
    } */










  }
}




