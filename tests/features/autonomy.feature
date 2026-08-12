Feature: Autonomous node operation
  A performer node should retain enough canonical state to continue when the coordinator disappears.

  Scenario: Node reuses its last canonical phrase
    Given a bass node has received a canonical phrase
    When the coordinator becomes unavailable
    Then the node can prepare another local variation
