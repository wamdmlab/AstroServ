#!/bin/bash


# if the template table is existing, the previous template is used.
if [ -f "../template_table/template" ]
then 
echo "A template Table is existing."
else
rm -rf ../template_table
mkdir ../template_table
file=$(ls ../catalog.csv/)
templateFile=`echo $file | awk '{gsub(substr($0,index($0,"sqd")),"template");print $0}'`
mv ../catalog.csv/$file ../template_table/$templateFile
fi
#awk '{$2=null;$24=null; print }' template_table/$file | awk -F "  " '{print $1" "$2}' >template_table/template 
#rm -rf template_table/$file
