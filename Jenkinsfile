pipeline {
  parameters {
    choice(name: "HOST", choices: ['aws-virginia', 'server'], description: "Choose host")
    string(name: "BRANCH", defaultValue: "dev", description: "Choose branch")
    string(name: "USER", defaultValue: "admin", description: "Username")
    password(name: "PASSWORD", defaultValue: "SECRET", description: "Password")
  }

  environment {
    CREDENTIALS = credentials("$USER")
    DIR_NAME = "hft_${BRANCH}"
  }

  agent {label "$HOST"}

  stages {


    stage("Authorization") {
    echo "Working on branch ${BRANCH_NAME}"
      when {
                equals(actual: "$PASSWORD", expected: "${CREDENTIALS_PSW}")
            }
            steps {
                echo "Starting authorization"
                script {
                    try {
                        if ("$PASSWORD" == "${CREDENTIALS_PSW}") {
                            echo 'Authorization successful'
                        }

                        else {
                            echo 'Wrong password, try again !'
                        }
                    } catch (Exception e) {
                        e.toString()
                        echo 'Cannot find given credentials, contact with administrator'

                    }
                }
            }
    }

    stage("Preparing workspace") {
      steps {
        sh '''
        echo whoami &&
        sudo su &&
        rm /opt/hft/* > /dev/null  2>&1 &&
        cp -r /home/obukowski/workspace/"$DIR_NAME"/* /opt/hft &&
        chmod +x /opt/hft/db_ex_connections/stop_all.sh &&
        /bin/bash /opt/hft/db_ex_connections/stop_all.sh
        '''

        script {
            try {
                sh '''
                rm /opt/hft/Jenkinsfile &&
                rm /opt/hft/Dockerfile
                '''
            } catch (Exception e) {
                e.toString()
                echo 'Files are already deleted'
            }
        }
      }
    }

    stage("Running workspace") {
      steps {
        sh '''
        sudo su &&
        chmod +x /opt/hft/db_ex_connections/start_all.sh &&
        /bin/bash /opt/hft/db_ex_connections/start_all.sh
        '''
      }
    }
  }
}




