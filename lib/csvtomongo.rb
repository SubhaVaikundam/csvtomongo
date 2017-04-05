require 'csv'
require 'json'
require 'mongo'
include Mongo

FILENAME="dcs_location.csv"


HEADER = ['location_id','ext_loc_id','type','name','latitude','longitude']

csv_data = CSV.read FILENAME

headers = HEADER.map {|i| i.to_s }
string_data = csv_data.map {|row| row.map {|cell| cell.to_s } }
array_of_hashes = string_data.map {|row| Hash[*headers.zip(row).flatten] }

# Instantiate a new cleinet
mongo_client= Mongo::Client.new([ 'localhost:27017', 'localhost:27017' ])


# Instantiate a new database
mongo_db = Mongo::Database.new(mongo_client, :storefinder)

# Instantiate a new collection
mongo_coll=Mongo::Collection.new(mongo_db, 'storelocation')


array_of_hashes.each do |td|

  # Sanity check debug, output each td message as nice looking JSON 
  #puts JSON.pretty_generate(td)   

  # insert into collection and return with the id 
  id = mongo_coll.insert_one(td)

end



