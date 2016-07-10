module StripeFS
	# Module provides common utility functions
	module Utils
		extend self	
		#	Check if variable is blank i.e. empty, nil	
		#	@param var [Object]
		#
		#	@return [Boolean] true or false
		def is_blank?(var)
 			var.respond_to?(:empty?) ? var.empty? : !var
		end

	end	# Utils
end	#	StripeFS
