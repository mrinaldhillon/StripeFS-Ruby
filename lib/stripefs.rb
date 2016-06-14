require 'optparse'
require_relative 'stripefs/utils'
require_relative 'stripefs/fs'

module StripeFS
	extend self	
	
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

			# No argument, shows at tail.  This will print an options summary.
			opts.on_tail("-h", "--help", "Show this message") do
				puts opts
				exit
			end
		end
		opt_parser.parse!(args)
		return chunksize,stripes
	end  # parse()

	def mount
		begin
			chunksize,stripes = parse(ARGV)
			exit if Utils.is_blank?(stripes)
			RFuse.main(ARGV) do |options, argv| 
				StripeFS::FS.new(options[:mountpoint], stripes, chunksize)
			end

		rescue RFuse::Error => error
			puts error.inspect
		end
	end
end
