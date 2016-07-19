Feature: Complex Spark

  In order to know about the houses in my area
  As Dan
  I want to be able to load house price data, join it to postcode location data, filter it by bounding box and save it


  Scenario: Joining data from two CSV files and creating a parquet file as output
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
    Then the parquet data written to "results.parquet" is
      | Price  | Postcode | HouseType | Latitude | Longitude |
      | 318000 | NN9 6LS  | D         | 51.1     | -1.2      |
      | 137000 | NN3 8HJ  | T         | 51.2     | -1.1      |
      | 180000 | NN14 6TN | S         | 51.3     | -1.0      |
      | 249000 | NN14 6TN | D         | 51.3     | -1.0      |

