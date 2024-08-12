cd ../..
rm GRADESCOPE.md
cd build
make submit-p4
cd ..
python3 gradescope_sign.py
mv project4-submission.zip /mnt/c/Users/shicaolong/Desktop
