from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# Defining the plugin class
class UserRecPlugin(AirflowPlugin):
    name = "user_rec_plugin"
    operators = [
        operators.ClosestConnectionOperator,
        operators.ReputationScoreOperator,
        operators.UserRecOperator,
        operators.LoadClosestConnectionOperator,
        operators.LoadUserRecOperator,
        operators.LoadUserRecReasonOperator,
        operators.DataQualityOperator
    ]
    helpers = [
        helpers.ClosestConnectionLogic,
        helpers.ReputationScoreLogic,
        helpers.UserRecLogic
    ]
