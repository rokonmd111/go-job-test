import os
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import time
from typing import Dict, Optional, Any
from datetime import datetime, timedelta
import json


# =========================================================
# কনফিগারেশন সেটিংস এবং API Endpoints
# =========================================================

BASE_URL = os.environ.get('BASE_URL', 'https://alljobs.teletalk.com.bd')
PDF_BASE_PATH = os.environ.get('PDF_BASE_PATH', '/media/') 

API_GOVT_LIST = os.environ.get('API_GOVT_LIST')
API_PRIVATE_LIST = os.environ.get('API_PRIVATE_LIST')
API_JOB_DETAILS = os.environ.get('API_JOB_DETAILS')

TARGET_API_URLS = {
    'Government': API_GOVT_LIST,
    'Private': API_PRIVATE_LIST
}

BLOG_ID = os.environ.get('BLOG_ID')

SCOPES = ['https://www.googleapis.com/auth/blogger']
# ⚠️ ডিলে ৩০ সেকেন্ডে উন্নীত করা হলো
DELAY_AFTER_OPERATION = 10 

# লেবেল ফরম্যাট
JOB_ID_LABEL_PREFIX = "JobID:"
END_DATE_LABEL_PREFIX = "EndDate:"
# API থেকে প্রাপ্ত তারিখের ফরম্যাট
API_DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ' 

# API কলের জন্য Headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Host': 'alljobs.teletalk.com.bd',
    'Referer': BASE_URL + '/',
}

# =========================================================
# সহায়ক ফাংশন
# =========================================================

def get_blogger_service() -> Optional[Any]:
    creds = None
    
    token_json_str = os.environ.get('GOOGLE_TOKEN_JSON')
    client_secret_json_str = os.environ.get('GOOGLE_CLIENT_SECRET_JSON')

    if token_json_str:
        token_info = json.loads(token_json_str)
        creds = Credentials.from_authorized_user_info(token_info, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not client_secret_json_str:
                print("Error: Client Secret Environment Variable missing!")
                return None
            
            client_config = json.loads(client_secret_json_str)
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)

    return build('blogger', 'v3', credentials=creds)

def format_api_date(date_str: str) -> str:
    """API থেকে পাওয়া UTC তারিখকে DD-MM-YYYY HH:MM AM/PM ফরম্যাটে রূপান্তর করে।"""
    if not date_str:
        return "N/A"
    try:
        dt_object = datetime.strptime(date_str, API_DATE_FORMAT)
        # UTC থেকে BDT (UTC+6) এ রূপান্তর
        dt_object_bdt = dt_object + timedelta(hours=6)
        return dt_object_bdt.strftime("%d-%m-%Y %I:%M %p")
    except ValueError:
        return date_str

def parse_end_date_for_check(date_str: str) -> Optional[datetime.date]:
    """ডিলিট করার লজিকের জন্য লেবেল থেকে তারিখ DD-MM-YYYY ফরম্যাটে পার্স করে।"""
    try:
        # লেবেলে DD-MM-YYYY ফরম্যাটে সেভ করা থাকে
        return datetime.strptime(date_str, '%d-%m-%Y').date()
    except ValueError:
        return None

# =========================================================
# ধাপ ১: API থেকে তালিকা ফেচ করা (সংস্থা ভিত্তিক)
# =========================================================

def fetch_job_list_from_page(session: requests.Session, api_url: str, page_num: int) -> Dict[str, Any]:
    """একটি নির্দিষ্ট API পৃষ্ঠা থেকে JSON ডেটা সংগ্রহ করে।"""
    params = {'page': page_num, 'limit': 20}
    try:
        response = session.get(api_url, headers=HEADERS, params=params, timeout=20)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        return {}


def fetch_all_target_jobs() -> Dict[str, Dict[str, Any]]:
    """সমস্ত API Endpoint থেকে সমস্ত পোস্টের তালিকা সংগ্রহ করে (প্রতিটি সংস্থাকে একটি পোস্ট হিসেবে)।"""
    print("\n▶️ ধাপ ৩: লক্ষ্য সাইট থেকে সমস্ত তালিকা সংগ্রহ শুরু (API Mode)...")
    all_jobs: Dict[str, Dict[str, Any]] = {} 
    session = requests.Session()
    time.sleep(2) 

    for job_type, api_base_url in TARGET_API_URLS.items():
        current_page = 1
        print(f"   🔎 Job Type: {job_type} ({api_base_url}) প্রক্রিয়াকরণ করা হচ্ছে...")
        
        while True:
            json_response = fetch_job_list_from_page(session, api_base_url, current_page)
            
            if job_type == 'Government':
                org_list = json_response.get('govtOrgJobs', [])
                nested_jobs_key = 'govt_jobs' 
            elif job_type == 'Private':
                org_list = json_response.get('privateRecruiterJobs', []) 
                if not org_list:
                    org_list = json_response.get('recruiterJobs', [])
                nested_jobs_key = 'private_jobs' 

            if not org_list:
                if current_page > 1:
                    print(f"      - Page {current_page}: কোনো সংস্থা নেই, শেষ পেজ।")
                break
            
            job_count_on_page = 0
            
            for org_item in org_list:
                org_name = org_item.get('name_bn') or org_item.get('name') or "অজানা সংস্থা"
                jobs_in_org = org_item.get(nested_jobs_key, [])
                
                if not jobs_in_org:
                    continue 
                    
                first_job_item = jobs_in_org[0] 
                main_job_id = str(first_job_item.get('id'))
                
                nested_titles = [
                    (job.get('job_title_bn') or job.get('job_title', 'পদবিহীন')).strip()
                    for job in jobs_in_org
                ]
                
                full_title = org_name.strip()
                details_url = f"{BASE_URL}/job/details/{main_job_id}?jobId={main_job_id}"
                
                if main_job_id and len(full_title) > 2:
                    all_jobs[main_job_id] = {
                        'title': full_title,
                        'url': details_url,
                        'nested_titles': nested_titles,
                        'job_type': job_type
                    }
                    job_count_on_page += 1
            
            current_page += 1
            time.sleep(1) 

    print(f"✅ লক্ষ্য সাইট থেকে সংগ্রহ সম্পন্ন। মোট {len(all_jobs)} টি পোস্ট (সংস্থা) পাওয়া গেছে।")
    return all_jobs


# =========================================================
# ধাপ ২: Job ID ব্যবহার করে বিস্তারিত ডেটা ফেচ করা (API Data Fetching)
# =========================================================

def fetch_job_details_by_id(session: requests.Session, job_id: str) -> Optional[Dict[str, str]]:
    """Job ID ব্যবহার করে বিস্তারিত API কল করে PDF Link, Dates, Description এবং Application Site সংগ্রহ করে।"""
    print(f"        ⚙️ বিস্তারিত API কল শুরু (ID: {job_id})...")
    api_url = f"{API_JOB_DETAILS}?id={job_id}"
    
    try:
        response = session.get(api_url, headers=HEADERS, timeout=20)
        response.raise_for_status()
        data = response.json()
        
        details = data.get('details', {})
        
        pdf_link = details.get('advertisement_file')
        start_date_str = details.get('published_date')
        end_date_str = details.get('deadline_date')
        # ✅ নতুন ডেটা: application_site ডেটা আনা
        application_site = details.get('application_site') 

        final_start_date = format_api_date(start_date_str)
        final_end_date = format_api_date(end_date_str)
        
        org_details = details.get('job_utilities_govtorganization', {})
        short_description = org_details.get('details') or "বিস্তারিত বিবরণ পাওয়া যায়নি।"

        # ⚠️ PDF লিংকের কাঠামো সংশোধন
        if pdf_link and not pdf_link.startswith('http'):
            # public/uploads/... এর আগে /media/ যোগ করা
            final_pdf_link = f"{BASE_URL}{PDF_BASE_PATH}{pdf_link}" 
        else:
            final_pdf_link = pdf_link
        
        if final_pdf_link and final_end_date != "N/A":
            print("        ✅ বিস্তারিত ডেটা সফলভাবে পাওয়া গেছে।")
            return {
                'pdf_link': final_pdf_link,
                'start_date': final_start_date,
                'end_date': final_end_date,
                'description': short_description,
                'application_site': application_site # ✅ নতুন ডেটা যোগ
            }
        else:
            print("        ❌ প্রয়োজনীয় ডেটা (PDF/End Date) পাওয়া যায়নি।")
            return None
            
    except Exception as e:
        print(f"        ❌ বিস্তারিত রিকোয়েস্ট/পার্সিং ব্যর্থ: {e}")
        return None

# =========================================================
# ধাপ ৩: ব্লগার পোস্ট ডেটা ফেচ করা
# =========================================================

def fetch_blogger_posts(service: Any, blog_id: str) -> Dict[str, Dict[str, Any]]:
    """ব্লগার ব্লগ থেকে বর্তমানে প্রকাশিত সমস্ত পোস্ট এবং তাদের metadata সংগ্রহ করে।"""
    print("\n▶️ ধাপ ১: ব্লগার থেকে বর্তমান পোস্টের তালিকা সংগ্রহ শুরু (ডিলিটের জন্য)...")
    published_jobs: Dict[str, Dict[str, Any]] = {}
    
    try:
        # maxResults=500 পর্যন্ত পোস্টে Job ID লেবেল আছে কিনা তা চেক করবে
        response = service.posts().list(blogId=blog_id, fetchBodies=False, maxResults=500).execute()
        posts = response.get('items', [])

        for post in posts:
            post_labels = post.get('labels', [])
            job_id = None
            end_date = None
            
            for label in post_labels:
                if label.startswith(JOB_ID_LABEL_PREFIX):
                    job_id = label[len(JOB_ID_LABEL_PREFIX):].strip()
                elif label.startswith(END_DATE_LABEL_PREFIX):
                    end_date = label[len(END_DATE_LABEL_PREFIX):].strip()
            
            if job_id:
                published_jobs[job_id] = {
                    'post_id': post['id'],
                    'title': post['title'],
                    'end_date': end_date,
                    'labels': post_labels
                }

    except Exception as e:
        print(f"❌ ব্লগার API থেকে ডেটা আনা ব্যর্থ হয়েছে: {e}")
    
    print(f"✅ ব্লগার থেকে সংগ্রহ সম্পন্ন। মোট {len(published_jobs)} টি Job ID যুক্ত পোস্ট পাওয়া গেছে।")
    return published_jobs

# =========================================================
# ধাপ ৪: ডিলিট লজিক (সংশোধিত: শুধুমাত্র ডিলিট)
# =========================================================

def delete_expired_posts(service: Any, blog_id: str, blogger_posts: Dict[str, Dict[str, Any]]):
    """ব্লগার পোস্টগুলো চেক করে মেয়াদ উত্তীর্ণ সরকারী পোস্ট ডিলিট করে।"""
    print("\n▶️ ধাপ ২: ডিলিট প্রক্রিয়া শুরু (মেয়াদ উত্তীর্ণ সরকারী পোস্ট)...")
    ids_to_delete = []
    current_date = datetime.now().date()
    
    # ডিলিট করার জন্য একটি অস্থায়ী তালিকা তৈরি করা
    for job_id, post_data in blogger_posts.items():
        
        # ১. 'সরকারী চাকরি' ট্যাগ আছে কি না তা পরীক্ষা করা
        is_govt_job = 'সরকারী চাকরি' in post_data.get('labels', [])

        # ২. মেয়াদ উত্তীর্ণ হয়েছে কি না তা পরীক্ষা করা (টার্গেটেড তারিখের একদিন পর)
        is_expired = False
        if post_data.get('end_date'):
            post_end_date = parse_end_date_for_check(post_data['end_date'])
            
            if post_end_date:
                deletion_date = post_end_date + timedelta(days=1) 
                
                if deletion_date <= current_date:
                    is_expired = True

        if is_govt_job and is_expired:
            ids_to_delete.append(job_id)

    if ids_to_delete:
        print(f"   🗑️ মোট {len(ids_to_delete)} টি মেয়াদ উত্তীর্ণ সরকারী পোস্ট ডিলিট করা হবে।")
        for job_id_to_delete in ids_to_delete:
            post_id = blogger_posts[job_id_to_delete]['post_id']
            try:
                service.posts().delete(blogId=blog_id, postId=post_id).execute()
                print(f"      - পোস্ট ID {post_id} ডিলিট সম্পন্ন।")
                
                # ডিলিট সম্পন্ন হলে এটিকে মূল ডিকশনারি থেকেও মুছে ফেলা হচ্ছে 
                # যাতে পরবর্তী অ্যাডিশন লজিকে এটি বিবেচনায় না আসে
                del blogger_posts[job_id_to_delete]
                
                time.sleep(DELAY_AFTER_OPERATION) # ডিলিট অপারেশনের পরে ডিলে
            except Exception as e:
                print(f"      ❌ ডিলিট ব্যর্থ হয়েছে: পোস্ট ID {post_id}. ত্রুটি: {e}")
    else:
        print("   ✅ কোনো ডিলিট করার মতো মেয়াদ উত্তীর্ণ সরকারী পোস্ট পাওয়া যায়নি।")


# =========================================================
# ধাপ ৫: নতুন পোস্ট যোগ করা (সংশোধিত: শুধুমাত্র অ্যাডিশন)
# =========================================================

def add_new_posts(service: Any, blog_id: str, target_posts: Dict[str, Dict[str, str]], blogger_posts: Dict[str, Dict[str, Any]]):
    """টার্গেট ও ব্লগারের পোস্টগুলো তুলনা করে শুধুমাত্র নতুন পোস্টগুলো যোগ করে।"""
    
    print("\n▶️ ধাপ ৪: নতুন পোস্ট প্রকাশের প্রক্রিয়া শুরু...")
    
    # যেগুলি টার্গেটে আছে কিন্তু ব্লগারের বর্তমান তালিকায় নেই (যা ডিলিটের পর আপডেট হয়েছে)
    titles_to_add = {id: data for id, data in target_posts.items() if id not in blogger_posts}

    session = requests.Session() 

    if titles_to_add:
        print(f"   ✍️ মোট {len(titles_to_add)} টি নতুন পোস্ট প্রকাশ করা শুরু হচ্ছে...")
        
        # ⚠️ নতুন পোস্টে পুরাতন পোস্টের আগে দেখানো নিশ্চিত করতে পোস্টগুলো বিপরীত ক্রমে লুপ করা হচ্ছে।
        posts_to_add_reversed = list(titles_to_add.items())
        posts_to_add_reversed.reverse()
        
        is_first_post = True
        
        for job_id, data in posts_to_add_reversed:
            
            # পোস্ট করার মাঝে ৩০ সেকেন্ডের ডিলে
            if not is_first_post:
                print(f"      ⏸️ পরবর্তী পোস্টের জন্য {DELAY_AFTER_OPERATION} সেকেন্ড অপেক্ষা করা হচ্ছে...")
                time.sleep(DELAY_AFTER_OPERATION)
            
            is_first_post = False
            
            # Job Details API কল
            details_data = fetch_job_details_by_id(session, job_id)
            
            if not details_data:
                print(f"      ❌ বিস্তারিত ডেটা আনতে ব্যর্থ: {data['title']}. এড়িয়ে যাওয়া হলো।")
                continue 

            final_end_date = details_data['end_date']
            final_start_date = details_data['start_date']
            final_pdf_link = details_data['pdf_link']
            description = details_data['description']
            application_site = details_data['application_site'] # ✅ নতুন ডেটা গ্রহণ
            nested_titles = data.get('nested_titles', []) 
            job_type = data.get('job_type', 'Unknown') 

            # Nested Titles তালিকা HTML তৈরি
            title_list_html = "<ul>" + "".join([f"<li>{t}</li>" for t in nested_titles]) + "</ul>"
            
            # ✅ অনলাইন আবেদনের বাটন তৈরি
            application_button_html = ""
            if application_site:
                application_button_html = f"""
            <div style="margin-top: 20px; text-align: center;">
                <a href="{application_site}" target="_blank" style="background-color: #4CAF50; color: white; padding: 15px 25px; text-align: center; text-decoration: none; display: inline-block; border-radius: 8px; font-size: 16px; font-weight: bold;">
                    অনলাইন আবেদনের লিংক
                </a>
            </div>
            <hr/>
            """
            

            # কন্টেন্ট তৈরি
            post_content = f"""
            {application_button_html} <div style="padding: 15px; border: 1px solid #007456; background-color: #f0fff0;">
                <h3 style="color: #007456; margin-top: 0;">পদসমূহের তালিকা</h3>
                {title_list_html}
            </div>
            <hr/>
            <div style="padding: 15px; border: 1px solid #ccc; background-color: #f9f9f9;">
                <h3 style="color: #007456; margin-top: 0;">আবেদনের সময়সীমা</h3>
                <p style="font-weight: bold;">শুরুর তারিখ: {final_start_date}</p>
                <p style="font-weight: bold; color: #CC0000;">শেষের তারিখ: {final_end_date}</p>
            </div>
            <hr/>
            <h3 style="color: #007456;">সার্কুলার PDF</h3>
            <p>বিস্তারিত সার্কুলার দেখতে ক্লিক করুন: <a href="{final_pdf_link}" target="_blank">{final_pdf_link}</a></p>
            <hr/>
            <h3 style="color: #007456;">সংস্থার বিবরণ</h3>
            <p>{description}</p>
            """
            
            # লেবেল তৈরি (ট্যাগ যুক্ত করা)
            post_labels = ['জব সার্কুলার'] 
            
            if job_type == 'Government':
                post_labels.append('সরকারী চাকরি')
            elif job_type == 'Private':
                post_labels.append('বেসরকারি চাকরি') 
                
            post_labels.append(f"{JOB_ID_LABEL_PREFIX}{job_id}")
            post_labels.append(f"{END_DATE_LABEL_PREFIX}{final_end_date.split(' ')[0]}") 

            post_body = {
                'kind': 'blogger#post',
                'title': data['title'], 
                'content': post_content,
                'labels': post_labels,
                'isDraft': False
            }
            
            # পোস্ট করা
            try:
                service.posts().insert(blogId=blog_id, body=post_body).execute()
                print(f"      ✅ সফলভাবে প্রকাশিত: {data['title']}")
            except Exception as e:
                print(f"      ❌ API ERROR: পোস্ট করার সময় ব্যর্থ: {data['title']}. ত্রুটি: {e}")
                
    else:
        print("   ✅ কোনো নতুন পোস্ট যোগ করার মতো পাওয়া যায়নি।")
        
    print("\n✅ সিঙ্ক্রোনাইজেশন প্রক্রিয়া সম্পন্ন হয়েছে।")


# =========================================================
# প্রধান নির্বাহ (Main Execution - সংশোধিত ও সঠিক ক্রম)
# =========================================================

def run_synchronization():
    """সিঙ্ক্রোনাইজেশন প্রক্রিয়া শুরু করে। (সঠিক ক্রম: ব্লগার ফেচ -> ডিলিট -> টার্গেট ফেচ -> অ্যাড)"""
    print("--- Teletalk Job Sync স্ক্রিপ্ট শুরু ---")
    
    blogger_service = get_blogger_service()
    if not blogger_service:
        print("❌ ব্লগার অথেন্টিকেশন ব্যর্থ। স্ক্রিপ্ট বাতিল করা হলো।")
        return
    
    # 1. ধাপ ১: ব্লগার থেকে বর্তমান পোস্টের তালিকা আনা
    blogger_posts = fetch_blogger_posts(blogger_service, BLOG_ID)

    # 2. ধাপ ২: মেয়াদ উত্তীর্ণ পোস্ট ডিলিট করা
    delete_expired_posts(blogger_service, BLOG_ID, blogger_posts) 
    
    # 3. ধাপ ৩: লক্ষ্য সাইট থেকে ডেটা আনা (নতুন পোস্টের জন্য)
    target_posts = fetch_all_target_jobs()
    if not target_posts:
        print("❌ টার্গেট সাইট থেকে কোনো পোস্ট ডেটা পাওয়া যায়নি। নতুন পোস্ট করার প্রক্রিয়া বাতিল করা হলো।")
        print("\n--- Teletalk Job Sync স্ক্রিপ্ট সমাপ্ত ---")
        return

    # 4. ধাপ ৪: নতুন পোস্ট যোগ করা 
    # (এখন blogger_posts-এ ডিলিট হওয়া পোস্টগুলোর ডেটা নেই, তাই এটি সঠিকভাবে নতুন পোস্টগুলো খুঁজে পাবে)
    add_new_posts(blogger_service, BLOG_ID, target_posts, blogger_posts)

    print("\n--- Teletalk Job Sync স্ক্রিপ্ট সমাপ্ত ---")


if __name__ == '__main__':
    run_synchronization()