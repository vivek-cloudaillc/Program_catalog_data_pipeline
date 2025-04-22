import asyncio
import json
import re
import aiohttp
import boto3
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup

# AWS Clients
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# S3 Configuration
BUCKET = "cloudaillc-vivek"
OUTPUT_PREFIX = "Program_Catalog_Pipeline/output/"
RAW_PREFIX = "Program_Catalog_Pipeline/raw/"
PDF_PREFIX = "Program_Catalog_Pipeline/program_pdfs/"
ALL_PROGRAMS_KEY = f"{OUTPUT_PREFIX}allprograms.json"

# DynamoDB Configuration
TABLE_NAME = "program_data"
table = dynamodb.Table(TABLE_NAME)

# Scrape list of programs
async def scrape_programs(session, url):
    async with session.get(url) as response:
        html = await response.text()

    soup = BeautifulSoup(html, "html.parser")
    programs = []
    filter_class = ".filter_8" if ".filter_8" in url else ".filter_2"

    for item in soup.select(f"li.item{filter_class}"):
        title_el = item.select_one("span.title")
        href_el = item.select_one("a")
        if not title_el or not href_el:
            continue
        title = title_el.text.replace("\u00a0", " ").strip()
        href = href_el.get("href")
        full_url = href if href.startswith("http") else f"https://catalog.odu.edu{href}"
        keywords = [kw.text.replace("\u00a0", " ").strip() for kw in item.select("span.keyword")]
        programs.append({
            "programTitle": title,
            "programUrl": full_url,
            "academicLevel": keywords[0].lower() if len(keywords) > 0 else "",
            "programType": keywords[1] if len(keywords) > 1 else "",
            "academicInterests": keywords[2] if len(keywords) > 2 else "",
            "collegesAndSchools": keywords[3] if len(keywords) > 3 else ""
        })

    return programs

# Scrape tab content and upload raw HTML
async def scrape_tab_content(session, program):
    try:
        async with session.get(program["programUrl"]) as response:
            html = await response.text()

        slug = program["programUrl"].rstrip("/").split("/")[-1]
        html = html.replace("\u00a0", " ")

        s3_client.put_object(
            Bucket=BUCKET,
            Key=f"{RAW_PREFIX}{slug}.html",
            Body=html,
            ContentType="text/html"
        )

        soup = BeautifulSoup(html, "html.parser")
        department = soup.select_one("#breadcrumb ul li:nth-of-type(4) a")
        program["department"] = department.text.replace("\u00a0", " ").strip() if department else ""

        tabs_data = {}
        pattern = re.compile(r"\b([A-Z]{2,4}\s?\d{3}[A-Z]?)\b(?!-level|/\d{3}-level)")

        if soup.select("#tabs"):
            for tab in soup.select("#tabs li[role='presentation']"):
                tab_name = tab.get_text(strip=True).replace("\u00a0", " ")
                content_id = tab.find("a").get("href", "").replace("#", "")
                content_div = soup.select_one(f"#{content_id}")
                if content_div:
                    tab_html = str(content_div).replace("\u00a0", " ")
                    tab_html = re.sub(r"\b[A-Z]{2,4}\s?\d{3}[A-Z]?-level\b", "", tab_html)
                    courses = list(set(pattern.findall(tab_html)))
                    tabs_data[tab_name] = {"content": tab_html, "courseExtractedFromText": courses}
        elif soup.select_one("#requirementstextcontainer"):
            raw = str(soup.select_one("#requirementstextcontainer")).replace("\u00a0", " ")
            raw = re.sub(r"\b[A-Z]{2,4}\s?\d{3}[A-Z]?-level\b", "", raw)
            courses = list(set(pattern.findall(raw)))
            tabs_data["Requirements"] = {"content": raw, "courseExtractedFromText": courses}
        elif soup.select_one("#textcontainer"):
            raw = str(soup.select_one("#textcontainer")).replace("\u00a0", " ")
            tabs_data["default"] = {"content": raw}

        program["tabs"] = tabs_data

    except Exception as e:
        print(f"❌ Error fetching tabs for {program['programTitle']}: {e}")
        program["department"] = ""
        program["tabs"] = {}

# Download PDF and upload to S3
async def download_pdf(session, program):
    slug = program["programUrl"].rstrip("/").split("/")[-1]
    pdf_url = f"{program['programUrl'].rstrip('/')}/{slug}.pdf"
    try:
        async with session.get(pdf_url) as response:
            if response.status == 200:
                s3_key = f"{PDF_PREFIX}{slug}.pdf"
                s3_client.put_object(
                    Bucket=BUCKET,
                    Key=s3_key,
                    Body=await response.read(),
                    ContentType="application/pdf"
                )
                print(f"✅ PDF uploaded: {slug}.pdf")
            else:
                print(f"❌ PDF not found: {slug}.pdf")
    except Exception as e:
        print(f"❌ PDF error for {slug}: {str(e)}")

# Add S3 URI to program if PDF exists
def add_pdf_s3_uri(programs):
    for program in programs:
        slug = program["programUrl"].rstrip("/").split("/")[-1]
        pdf_key = f"{PDF_PREFIX}{slug}.pdf"
        try:
            s3_client.head_object(Bucket=BUCKET, Key=pdf_key)
            program["ProgramS3uri"] = f"s3://{BUCKET}/{pdf_key}"
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                program["ProgramS3uri"] = ""
            else:
                print(f"⚠️ Error checking PDF for {slug}: {str(e)}")
                program["ProgramS3uri"] = ""

# Read JSON from S3
def read_data_from_s3(bucket_name, file_key):
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except ClientError as e:
        print(f"Error reading S3 file {file_key}: {e}")
        return None

# Load records into DynamoDB
def load_data_to_dynamodb(data):
    success_count = 0
    failed_count = 0
    failed_items = []

    for record in data:
        try:
            if "programTitle" not in record or "department" not in record:
                print(f"Skipping record due to missing keys: {record}")
                failed_count += 1
                continue

            record["department"] = record["department"] or "Not Provided"
            table.put_item(Item=record)
            success_count += 1
        except Exception as e:
            print(f"Error inserting record: {str(e)}")
            failed_count += 1
            failed_items.append(record)

    return success_count, failed_count, failed_items

# Main async runner
async def run():
    urls = [
        "https://catalog.odu.edu/programs/#filter=.filter_8",
        "https://catalog.odu.edu/programs/#filter=.filter_2"
    ]
    all_programs = []

    async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
        for url in urls:
            programs = await scrape_programs(session, url)
            all_programs.extend(programs)

        # Deduplicate
        seen = set()
        unique_programs = []
        for p in all_programs:
            if p["programUrl"] not in seen:
                seen.add(p["programUrl"])
                unique_programs.append(p)

        await asyncio.gather(*[scrape_tab_content(session, p) for p in unique_programs])
        await asyncio.gather(*[download_pdf(session, p) for p in unique_programs])

    # Clean all fields globally from \u00a0
    for program in unique_programs:
        for key in program:
            if isinstance(program[key], str):
                program[key] = program[key].replace("\u00a0", " ")

    add_pdf_s3_uri(unique_programs)

    # Upload final cleaned JSON
    s3_client.put_object(
        Bucket=BUCKET,
        Key=ALL_PROGRAMS_KEY,
        Body=json.dumps(unique_programs, indent=2),
        ContentType="application/json"
    )
    print("✅ Uploaded cleaned allprograms.json")

    # Load into DynamoDB
    data = read_data_from_s3(BUCKET, ALL_PROGRAMS_KEY)
    if data:
        success, failed, failed_records = load_data_to_dynamodb(data)
        print(f"📥 DynamoDB Insert: {success} success, {failed} failed.")
    else:
        print("❌ Failed to read allprograms.json from S3 for DynamoDB insert.")

# Lambda entry
def lambda_handler(event, context):
    asyncio.run(run())
    return {
        "statusCode": 200,
        "body": "Catalog processed and pushed to DynamoDB successfully."
    }
