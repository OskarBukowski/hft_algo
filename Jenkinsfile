pipeline {
  parameters {
    choice(name: "HOST", choices: ['aws-virginia', 'server'], description: "Choose host")
    string(name: "USER", defaultValue: "admin", description: "Username")
    password(name: "PASSWORD", defaultValue: "password", description: "Password")
  }

  environment {
    CREDENTIALS = credentials("$USER")
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
        sudo rm -rf /opt/hft/* > /dev/null  2>&1 &&
        sudo mkdir /opt/hft/db_ex_connections &&
        sudo cp -r /home/obukowski/workspace/hft_"${BRANCH_NAME}"/db_ex_connections/* /opt/hft/db_ex_connections &&
        sudo chmod +x /opt/hft/db_ex_connections/stop_all.sh &&
        /bin/bash /opt/hft/db_ex_connections/stop_all.sh
        '''

        script {
            try {
                sh '''
                sudo rm /opt/hft/Jenkinsfile &&
                sudo rm /opt/hft/Dockerfile
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
        sudo chmod +x /opt/hft/db_ex_connections/start_all.sh &&
        /bin/bash /opt/hft/db_ex_connections/start_all.sh
        '''
      }
    }
  }
}




