pipeline {
  parameters {
    choice(name: "HOST", choices: ['aws-virginia', 'other'], description: "Choose host")
    string(name: "BRANCH", defaultValue: "", description: "Choose branch")
    string(name: "USER", defaultValue: "", description: "Username")
    string(name: "PASSWORD", defaultValue: "", description: "Password")
  }

  environment {
    CREDENTIALS = credentials("$USER")
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
  }
}




