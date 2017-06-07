# redshift-create-manifest
Redshift script to create a MANIFEST file recursively.

# Exemple
`python create-manifest --bucket_in="myBucket" --bucket_out="myBucket" --prefix_in="my/path/" --prefix_out="out/manifest"`
   
This line will look for all "gz" files stored recursively in `my/path` prefix and store the created MANIFEST file into `out/manifest/manifest`
