Feature: Complex Spark

  In order to know about the houses in my area
  As Dan
  I want to be able to load house price data, join it to postcode location data, filter it by bounding box and save it


  Array([NN14 6TN,180000,S,51.3,-1.0], [NN14 6TN,249000,D,51.3,-1.0], [NN3 8HJ,137000,T,51.2,-1.1], [NN9 6LS,318000,D,51.1,-1.2])
  Array([318000,NN9 6LS,D,51.1,-1.2], [137000,NN3 8HJ,T,51.2,-1.1], [180000,NN14 6TN,S,51.3,-1.0], [249000,NN14 6TN,D,51.3,-1.0])


  Scenario: Joining data from two data frames to create a new data frame of results
    Given a table of data in a temp table called "housePrices"
      | Price:Int  | Postcode:String | HouseType:String |
      | 318000     | NN9 6LS         | D                |
      | 137000     | NN3 8HJ         | T                |
      | 180000     | NN14 6TN        | S                |
      | 249000     | NN14 6TN        | D                |
    And a table of data in a temp table called "postcodes"
      | Postcode:String | Latitude:Double | Longitude:Double |
      | NN9 6LS         | 51.1            | -1.2             |
      | NN3 8HJ         | 51.2            | -1.1             |
      | NN14 6TN        | 51.3            | -1.0             |
    When I join the data
    Then the data in temp table "results" is
      | Price:Int  | Postcode:String | HouseType:String | Latitude:Double | Longitude:Double |
      | 318000     | NN9 6LS         | D                | 51.1            | -1.2             |
      | 137000     | NN3 8HJ         | T                | 51.2            | -1.1             |
      | 180000     | NN14 6TN        | S                | 51.3            | -1.0             |
      | 249000     | NN14 6TN        | D                | 51.3            | -1.0             |


  Scenario: Joining data from temp tables and creating a parquet file as output
    Given a table of data in a temp table called "housePrices"
      | Price:Int  | Postcode:String | HouseType:String |
      | 318000     | NN9 6LS         | D                |
      | 137000     | NN3 8HJ         | T                |
      | 180000     | NN14 6TN        | S                |
      | 249000     | NN14 6TN        | D                |
    And a table of data in a temp table called "postcodes"
      | Postcode:String | Latitude:Double | Longitude:Double |
      | NN9 6LS         | 51.1            | -1.2             |
      | NN3 8HJ         | 51.2            | -1.1             |
      | NN14 6TN        | 51.3            | -1.0             |
    When I join the data and save to parquet
    Then the parquet data written to "results.parquet" is
      | Price:Int  | Postcode:String | HouseType:String | Latitude:Double | Longitude:Double |
      | 318000     | NN9 6LS         | D                | 51.1            | -1.2             |
      | 137000     | NN3 8HJ         | T                | 51.2            | -1.1             |
      | 180000     | NN14 6TN        | S                | 51.3            | -1.0             |
      | 249000     | NN14 6TN        | D                | 51.3            | -1.0             |

