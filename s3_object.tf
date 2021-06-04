resource "aws_s3_bucket_object" "prospect" {
    bucket = data.aws_s3_bucket.glue_scripts_bucket.id
    key = "admin/prospect"
    source = "glue_src/prospect.py"
    etag = filemd5 ("glue_src/prospect.py")

}

resource "aws_s3_bucket_object" "model" {
    bucket = data.aws_s3_bucket.glue_scripts_bucket.id
    key = "admin/model"
    source = "glue_src/model.py"
    etag = filemd5 ("glue_src/model.py")

}

resource "aws_s3_bucket_object" "alsac_territory" {
    bucket = data.aws_s3_bucket.structured_bucket.id
    key = "/DATADB/territory/alsac_territory.csv"
    source = "s3_files/alsac_territory.csv"
    etag = filemd5 ("s3_files/alsac_territory.csv")

}