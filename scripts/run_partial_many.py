import brukerbridge as bridge

extensions_for_oak_transfer = ['.nii', '.csv', '.xml', 'json', 'hdf5', 'tiff', '.txt'] # last 4 chars

### Directory on this computer to process ###
# full_target = 'H:/Ashley/' + str(date)
# full_target = 'G:/Max/20220301'
#full_target = 'H:/Michelle/220427'
#full_target = 'H:/Tim/20230329a'
# full_target = 'H:/luke/20230303__error__'
# full_target = 'H:/Minseung/visualattention'
#full_target = 'H:/Emma/20221006__queue__'
#full_target = 'H:/Alex/230211'
#full_target = 'H:/Arnaldo/20221011_elavGAL4_3rd_UAS-G6smlsTdtom_2nd_test_run'
#full_target = 'H:/Avery/20230302/'


### Oak target ###
#oak_target = 'X:/data/Brezovec/2P_Imaging/imports'
# oak_target = 'X:/data/Ashley2/imports'
#oak_target = 'X:/data/Emma/BrukerImaging/imports'
#oak_target = 'X/data/Ina/bruker_data/imports'
#oak_target = 'X:/data/krave/bruker_data/imports/20221129.test_fix/'
#oak_target = 'X:/data/Michelle/Bruker/imports'
#oak_target = 'X:/data/Tim/ImagingData/imports'
# oak_target = 'X:/data/Brezovec/2P_Imaging/imports'
# oak_target = 'X:/data/minseung/bruker_data/imports'
#oak_target = 'X:/data/Yukun/2P_Imaging/imports'
# oak_target = 'X:/data/Arnaldo/2P_Imaging/imports'
oak_target = 'X:/data/Jacob/ImagingData/Bruker/imports'



dates = ['20230814', "20230815", "20230816", "20230825", "20230829"]

for date in dates:
	#full_target = 'H:/Ashley/' + str(date)
	full_target = 'H:/Jacob/' + str(date)

	### raw to tiff ###
	#bridge.convert_raw_to_tiff(full_target)

	### tiffs to nii or tiff stack ###
	# bridge.convert_tiff_collections_to_nii(full_target)
	bridge.convert_tiff_collections_to_nii_split(full_target)
	#bridge.convert_tiff_collections_to_stack(full_target)

	### Transfer fictrac ###
	# user = 'luke'
	# bridge.transfer_fictrac(user)

	### Transfer to oak ###
	#bridge.start_oak_transfer(full_target, oak_target, extensions_for_oak_transfer, add_to_build_que="False")
