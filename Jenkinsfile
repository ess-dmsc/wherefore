@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

container_build_nodes = [
  'centos7': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc11')
]

pipeline_builder = new PipelineBuilder(this, container_build_nodes)
pipeline_builder.activateEmailFailureNotifications()

builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: Checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage

  pipeline_builder.stage("${container.key}: Dependencies") {
    container.sh """
      which python
      python --version
      python -m pip install --user --upgrade pip
      python -m pip install --user -r ${pipeline_builder.project}/requirements-dev.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Formatting") {
    container.sh """
      cd ${pipeline_builder.project}
      python -m black --check .
    """
  } // stage

  pipeline_builder.stage("${container.key}: Static Analysis") {
    // E203 formatting handled by black
    // E501 formatting handled by black
    // W503 complains about splitting if across lines which conflicts with black
    // W605 invalid escape sequence '\d'
    container.sh """
      cd ${pipeline_builder.project}
      python -m flake8 --ignore=E203,E501,W503,W605 test/ wherefore/ *.py --exclude=WhereforeGUI.py
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    container.sh """
      cd ${pipeline_builder.project}
      python -m pytest --cov --cov-report=html --cov-report=term .
      tar czf htmlcov.tar.gz htmlcov
    """
    container.copyFrom("${pipeline_builder.project}/htmlcov.tar.gz", ".")
    archiveArtifacts "htmlcov.tar.gz"
  } // stage
}  // createBuilders

node {
  dir("${pipeline_builder.project}") {
    scm_vars = checkout scm
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  cleanWs()
}
