
echo "-------------------------------------------------------------------------"
echo "Unzipping files."
echo "-------------------------------------------------------------------------"

 for ZIP_FILE in $(find ${ZIP_DIR} -name '*.zip')
 do
   UNZIP_FILE=$(basename ${ZIP_FILE} .zip)
   UNZIP_FILE="${UNZIP_DIR}/${UNZIP_FILE}.txt"
   # Only unzip if not already unzipped. This check assumes that x.zip unzips to
   # x.txt, which so far seems to be the case.
   if [ ! -f "${UNZIP_FILE}" ] ; then
     unzip -o "${ZIP_FILE}" -d "${UNZIP_DIR}"
   else
     echo "${ZIP_FILE##*/} already unzipped. Skipping."
   fi
 done