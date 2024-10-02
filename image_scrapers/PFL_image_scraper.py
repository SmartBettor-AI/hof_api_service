import requests
from bs4 import BeautifulSoup
import os

# URL of the page to scrape
url = "https://pflmma.com/regular-fighter-roster/all"

# Send a GET request to the page
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the page content with Beautiful Soup
    soup = BeautifulSoup(response.text, 'html.parser')

    # Create the directory if it doesn't exist
    os.makedirs('PFL_images', exist_ok=True)

    # Find all divs with the class "row"
    row = soup.find_all('div', class_='row')
    print(row[4])
    image_row = row[4]

    # Iterate through each row
 
        # Find all columns within the row
    fighters = image_row.find_all('div', class_='col-lg-3 col-md-4 col-6 mb-4')
    
        
        # Iterate through each column
    for fighter in fighters:
            # Find the fighter name
            fighter_name_tag = fighter.find('span', class_='fighter_name')
            fighter_name = fighter_name_tag.text.strip() if fighter_name_tag else None
            
            # Find the headshot image that is not the flag
            headshot = fighter.find('div', class_='fighter-headshot')
            if headshot and fighter_name:
                img_tag = headshot.find('img', class_='pt-3 w-100')
                if img_tag:
                    # Get the src attribute of the image
                    img_url = img_tag['src']
                    
                    # Prepare the filename
                    formatted_name = fighter_name.replace(" ", "_") + ".jpg"
                    file_path = os.path.join('PFL_images', formatted_name)
                    
                    # Download the image
                    img_data = requests.get(img_url).content
                    with open(file_path, 'wb') as f:
                        f.write(img_data)
                    print(f"Downloaded: {file_path}")

else:
    print(f"Failed to retrieve page. Status code: {response.status_code}")
