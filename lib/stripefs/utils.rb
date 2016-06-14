module StripeFS
	module Utils
		extend self	
		
		def is_blank?(var)
 			var.respond_to?(:empty?) ? var.empty? : !var
		end

	end
end
