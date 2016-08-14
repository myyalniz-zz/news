#!/usr/bin/ruby
# Written by Mustafa Yalniz
# 13 August 2016
# 

require 'net/http'
require 'yaml'
require 'fileutils'
require 'zip'
require 'redis'

config = YAML.load_file('config.yml')

puts "The zip files will be extracted to " + config['zip_temp_extract_location']

if ! Dir.exists?(config['zip_temp_extract_location']) 
    Dir.mkdir(config['zip_temp_extract_location'])
end

if Dir.exists?(config['zip_temp_extract_location'] + '/tempextract' )
    FileUtils.rm_r(config['zip_temp_extract_location'] + '/tempextract' ) 
end

# Set debug from config file
@debug = config['debug']

# Initializing redis
@redis_obj = Redis.new(:host => '127.0.0.1', :port => 6379)

# We assume, we know the structure and there is only one href in bitly.com body
# This is the base location for zip files 
response = Net::HTTP.get_response(URI(config['zip_files_url']))
body_of_response = response.body
zip_base_url =  body_of_response.scan(/href=\"([^\"]+)\"/)[0][0]

# Here is the real url to have zip files listed in href's
response_zip_repository = Net::HTTP.get_response(URI(zip_base_url.to_s))
body_of_zip_repository = response_zip_repository.body

# Get those zip file names into an array
zip_names = body_of_zip_repository.scan(/href=\"([^\"]+\.zip)\"/)

puts "There are " + zip_names.length.to_s + " zip files on this site!"


def place_in_redis(xml_file, redis_list_name)
    xml_content = File.read(xml_file)
 
    # We assume the topic_url is unique for each xml file
    topic_url = xml_content.scan(/\<topic_url\>([^\<]+)\<\/topic_url\>/)[0][0]
    
    if ! topic_url.nil?
    	domain = topic_url.scan(/http.?\:\/\/([^\/]+)\/?.*/)[0][0]

	if @redis_obj.sismember(domain.to_s, topic_url.to_s)
	    puts "Topic listed in " + topic_url.to_s + " is already stored" if @debug
  	    puts "Skip processing : " + xml_file + "..."
	else 
	    puts "Topic listed in " + topic_url.to_s + " has NOT been stored. Processing..." if @debug 
 	    @redis_obj.lpush("NEWS_XML", xml_content)
  	    puts "New xml content : " + xml_file + "..."
	    @redis_obj.sadd(domain.to_s, topic_url.to_s)
	end
	
    else
	xml_content
    end

end

def process_xmls(xml_directory, redis_list_name)

    Dir.glob(xml_directory + "/*.xml") do |xml_file|
	place_in_redis(xml_file, redis_list_name )
    end

end

def extract_zipfile(zip_file, zip_content_destination)
    FileUtils.mkdir_p(zip_content_destination)

    Zip::File.open(zip_file) do |xml_file|
      xml_file.each do |f|
        fpath = File.join(zip_content_destination, f.name)
        xml_file.extract(f, fpath) unless File.exist?(fpath)
      end
    end
end

def process_zip_file(zip_temp_extract_location, zip_file, redis_list_name)

	puts "Accessing to zip file location " + zip_temp_extract_location
	if ! Dir.exists?(zip_temp_extract_location + '/tempextract') 
		Dir.mkdir(zip_temp_extract_location + '/tempextract' ) 
        end
        extract_zipfile(zip_temp_extract_location.to_s + "/" + zip_file, zip_temp_extract_location.to_s + '/tempextract' )
	process_xmls(zip_temp_extract_location.to_s + '/tempextract', redis_list_name)
	if Dir.exists?(zip_temp_extract_location + '/tempextract') 
		FileUtils.rm_r(zip_temp_extract_location + '/tempextract' ) 
        end
	
end

# Process recursivelly all of the zip files on site html
zip_names.each { |zip_name| 
    # zip_name is an array with only 1 element
    zip_base_name = zip_name[0]
    puts "Processing " + zip_base_name.to_s
    File.write(config['zip_temp_extract_location'] + "/" + zip_base_name.to_s, \
	       Net::HTTP.get(URI.parse(zip_base_url.to_s + zip_base_name.to_s )))

    process_zip_file(config['zip_temp_extract_location'], zip_base_name.to_s, "NEWS_XML")

    # Remove zip file after processing
    File.delete(config['zip_temp_extract_location'] + "/" + zip_base_name.to_s)
}
