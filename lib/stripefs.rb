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
	#	@example
	#		StripeFS::CMD.mount(["/mnt/stripefs, "-s", "~/stripes/1", "-s", 
	#									"~/stripes/2", "-s", "~/stripes/3", "-c" "4096"])
	module CMD
		extend self	
		# Parse options
		# @param [Array<String>] command line arguments
		# 
		#	@return [Fixnum, Array<String>]	containing chunksize and stripe paths
		# @note method is written to handle command line arguments ARGV.
		def parse(args)
			chunksize = 1024	#default chunksize is 1024
			stripes = []

			opt_parser = OptionParser.new do |opts|
				opts.banner = "Usage: stripefs [options]"
				opts.separator ""
				opts.separator "Specific options:"
				opts.on("-s", "--stripe String", String, "Stripe path") { |str| stripes << str }
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

		# Mount StripeFS file system
		#	@param argv [Array<String>] command line options
		def mount(argv)
			begin
				chunksize,stripes = parse(argv)
				
				if Utils.is_blank?(stripes) || stripes.count <= 1
					parse(["-h"])
					
					fail ArgumentError, "Number of Stripe should be greater than 1"
				end
		
				RFuse.main(argv) do |options, args| 
					FS.new(options[:mountpoint], stripes, chunksize)
				end

			rescue => error
				$stderr << error
				fail error
			end
		end
	end # CMD
end # StripeFS
