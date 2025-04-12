locals {
  env_file                       = file("../nextjs/.env")
  google_application_credentials = file("../nextjs/clientLibraryConfig-aws-provider.json")
}

resource "aws_iam_instance_profile" "ec2_instance_profile" {
  name = "ec2-instance-profile"
  role = aws_iam_role.ec2_instance_role.name
}

resource "aws_instance" "nextjs" {
  ami                         = "ami-00a929b66ed6e0de6"
  instance_type               = "t2.micro"
  subnet_id                   = aws_subnet.public_subnet.id
  vpc_security_group_ids      = [aws_security_group.ec2_security_group.id]
  associate_public_ip_address = true
  iam_instance_profile        = aws_iam_instance_profile.ec2_instance_profile.name

  user_data_replace_on_change = true
  user_data                   = <<-EOT
    #!/bin/bash
    set -e

    # Install Node.js and Git
    curl -sL https://rpm.nodesource.com/setup_18.x | sudo bash -
    sudo yum install -y nodejs git

    # Set up GCP credentials
    cat <<EOF > /home/ec2-user/clientLibraryConfig-aws-provider.json
    ${local.google_application_credentials}
    EOF
    chmod 600 /home/ec2-user/clientLibraryConfig-aws-provider.json
    export GOOGLE_APPLICATION_CREDENTIALS=/home/ec2-user/clientLibraryConfig-aws-provider.json
    export GCP_PROJECT_ID=${var.GCP_PROJECT_ID}

    # Clone the Next.js repository
    cd /home/ec2-user
    git clone https://github.com/tnguyen2907/flight-delay-prediction.git
    cd flight-delay-prediction/nextjs
    
    # Set up environment variables
    cat <<EOF > .env
    ${local.env_file}
    EOF
    chmod 600 .env

    chown -R ec2-user:ec2-user /home/ec2-user/

    # Install dependencies and build the Next.js app
    npm install
    npm run build

    # Start the Next.js app
    nohup npm start -- --port 80 >/home/ec2-user/nextjs.log 2>&1 &
  EOT

  depends_on = [aws_security_group.ec2_security_group, aws_iam_instance_profile.ec2_instance_profile]
}
