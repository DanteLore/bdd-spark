Feature: Say hello to the world

  In order to be polite
  As a developer
  I want to say hello to the world

  Scenario: Add two numbers
    Given a calculator
    When I add '1' and '1'
    Then the result is '2'

  Scenario: Add two other numbers
    Given a calculator
    When I add '6' and '3'
    Then the result is '9'