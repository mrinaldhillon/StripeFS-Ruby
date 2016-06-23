require 'optparse'
require_relative 'stripefs/utils'
require_relative 'stripefs/fs'

#	StripeFS is stackable striping file system
#	
#	@author	Mrinal Dhillon
#	@email mrinaldhillon@gmail.com
module StripeFS
	# Command line utility to mount StripeFS FileSystem
	#
	#	@author Mrinal Dhillon
	#	@example
	#		bin/stripefs /home/mrinal/splitfs -s /home/mrinal/clmnt/1 -s /home/mrinal/clmnt/2 
	#																								-s /home/mrinal/clmnt/3 -c 4096	
	module CMD
		extend self	
		# Parse command line options
		# @param [Array<String>] args command line arguments
		# 
		#	@return [Fixnum, Array<String>]	containing chunksize and stripe paths
		def parse(args)
			chunksize = 1024		#default chunksize is 1024
			stripes = []

			opt_parser = OptionParser.new do |opts|
				opts.banner = "Usage: example.rb [options]"
				opts.separator ""
				opts.separator "Specific options:"
				opts.on("-s", "--stripe String", String, "Single Stripe Path") { |str| stripes << str }
				opts.on("-c", "--chunksize N", Integer, "Chunksize") { |chk| chunksize = chk } 
				opts.separator ""
				opts.separator "Common options:"

				opts.on_tail("-h", "--help", "Show this message") do
					$stdout << opts.to_s
					exit
				end
			end

			opt_parser.parse!(args)
			return chunksize, stripes
		end

		# Mount file system
		def mount
			begin
				chunksize,stripes = parse(ARGV)
				
				if Utils.is_blank?(stripes) || stripes.count <= 1 
					fail ArgumentError, "Number of Stripe should be greater than 1"
				end
		
				RFuse.main(ARGV) do |options, argv| 
					FS.new(options[:mountpoint], stripes, chunksize)
				end

			rescue => error
				$stderr << error
				fail error
			end
		end
	end	# CMD
end #StripeFS
