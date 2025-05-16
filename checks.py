import requests
from requests.auth import HTTPBasicAuth
import os
from dotenv import load_dotenv

load_dotenv()

ZENDESK_DOMAIN = os.getenv("ZENDESK_DOMAIN")  
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")    
ZENDESK_API_TOKEN = os.getenv("ZENDESK_API_TOKEN") 

auth = HTTPBasicAuth(f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)
headers = {
    "Content-Type": "application/json"
}

def get_contacts():
    url = f"https://{ZENDESK_DOMAIN}/api/v2/users.json"
    response = requests.get(url, auth=auth, headers=headers)
    return response.json()

def get_companies():
    url = f"https://{ZENDESK_DOMAIN}/api/v2/organizations.json"
    response = requests.get(url, auth=auth, headers=headers)
    return response.json()

def get_tickets_with_comments():
    tickets_url = f"https://{ZENDESK_DOMAIN}/api/v2/tickets.json"
    tickets_response = requests.get(tickets_url, auth=auth, headers=headers)
    tickets = tickets_response.json().get('tickets', [])

    ticket_details = []
    for ticket in tickets:
        ticket_id = ticket['id']
        comments_url = f"https://{ZENDESK_DOMAIN}/api/v2/tickets/{ticket_id}/comments.json"
        comments_response = requests.get(comments_url, auth=auth, headers=headers)
        comments = comments_response.json().get('comments', [])
        ticket_details.append({
            'ticket': ticket,
            'comments': comments
        })
    return ticket_details

def get_knowledge_base_articles():
    url = f"https://{ZENDESK_DOMAIN}/api/v2/help_center/en-us/articles.json"
    response = requests.get(url, auth=auth, headers=headers)
    return response.json()

# Example usage
if __name__ == "__main__":
    print("Fetching contacts...")
    contacts = get_contacts()
    print(contacts)

    print("\nFetching companies...")
    companies = get_companies()
    print(companies)

    print("\nFetching tickets with replies/notes...")
    tickets = get_tickets_with_comments()
    print(tickets)

    print("\nFetching Knowledge Base (Help Center) articles...")
    kbase = get_knowledge_base_articles()
    print(kbase)
