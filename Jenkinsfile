pipeline {
  parameters {
    choice(name: "HOST", choices: ['aws-virginia', 'server'], description: "Choose host")
    string(name: "BRANCH", defaultValue: "", description: "Choose branch")
    string(name: "USER", defaultValue: "", description: "Username")
    password(name: "PASSWORD", defaultValue: "SECRET", description: "Password")
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
        sudo su &&
        chmod +x /opt/hft/db_ex_connections/stop_all.sh &&
        cd /opt &&
        bash /opt/hft/db_ex_connections/stop_all.sh &&
        cp -r /home/ubuntu/workspace/"$DIR_NAME"/* /opt/hft &&
        '''

        script {}
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
        cd /opt &&
        chmod +x /opt/hft/db_ex_connections/start_all.sh &&
        bash /opt/hft/db_ex_connections/start_all.sh
        '''
      }
    }
  }
}




