To install dependencies


source venv/bin/activate
pip install -r requirements.txt

## run webscrapping 

After puttideploying the resourresources through transformer - please exxcute this to upload files in to S3
------------------------------------------------------------------------------------------------------------------
& "C:\Users\vijay\AppData\Local\Programs\Python\Python313\python.exe" "C:\A-Rearc\Code_for_rearc\Rearc_projects-main\src\rearc\Part1-SourcingDatatsets\rearc_datal_s3_sync.py" --base-url "http://download.bls.gov/pub/time.series/pr/" --bucket "vm-rearc-data" --prefix "bls/pr/" --region "us-east-1" --concurrency 8 --delete
--------------------------------------------------------------------------------------------------------------------


---------------------------Terraform Commands----------------------------------------------------
terraform fmt
terraform init -reconfigure
terraform init
terraform validate
terraform fmt -recurrsive
terraform plan --help
terraform plan -out tf.plan
terraform apply tf.plan
terraform destroy
terraform init -upgrade
terraform plan
terraform apply
terraform output

----------------------------------------------------------------------------------------------------
