import os
from pypdf import PdfReader
import fitz
from sqlalchemy import create_engine, text
from DB_details import db_params

def table_create():
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"

    create_table_query = """
    CREATE TABLE IF NOT EXISTS Pdfdata (
        id SERIAL PRIMARY KEY, 
        Page_no INT NOT NULL, 
        Data TEXT NOT NULL, 
        Image VARCHAR(10)
    );
    """
    
    try:
        print(f"Connecting to the database at {db_url}")
        engine = create_engine(db_url)
        
        with engine.connect() as connection:
            print("Executing table creation query.")
            result = connection.execute(text(create_table_query))
            print("Table 'Pdfdata' created or already exists.")
            
    except Exception as error:
        
        print(f"Error while creating the table: {error}")


def insert_page_data(page_num, text_data, image_exists):
    db_url = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    insert_query = """
    INSERT INTO Pdfdata (Page_no, Data, Image) 
    VALUES (:page_no, :data, :image);
    """
    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            connection.execute(text(insert_query), {"page_no": page_num, "data": text_data, "image": image_exists})
        print(f"Data for page {page_num} inserted successfully.")
    except Exception as error:
        print(f"Error inserting data for page {page_num}: {error}")


def extract_images(pdf_path, images_output_folder):
    pdf_file = fitz.open(pdf_path)
    image_exists = []
    
    for page_index in range(len(pdf_file)):
        page = pdf_file.load_page(page_index)
        image_list = page.get_images(full=True)
        
        if image_list:
            image_exists.append("Yes")  
            print(f"[+] Found a total of {len(image_list)} images on page {page_index}")
        else:
            image_exists.append("No")  
            print("[!] No images found on page", page_index)
        
    
        for image_index, img in enumerate(image_list, start=1):
            xref = img[0]
            base_image = pdf_file.extract_image(xref)
            image_bytes = base_image["image"]
            image_ext = base_image["ext"]
            image_name = f"image{page_index + 1}_{image_index}.{image_ext}"
            image_path = os.path.join(images_output_folder, image_name)
            with open(image_path, "wb") as image_file:
                image_file.write(image_bytes)
                print(f"[+] Image saved as {image_name}")
    
    return image_exists

def extract_text_and_save_to_db(pdf_path, text_output_path, images_output_folder):
    os.makedirs(images_output_folder, exist_ok=True)
    

    reader = PdfReader(pdf_path)
    image_exists = extract_images(pdf_path, images_output_folder)
    
    all_page_data = []  
    
   
    for page_num, page in enumerate(reader.pages):
        text_data = page.extract_text()  
        all_page_data.append({
            "page_num": page_num + 1,
            "text_data": text_data, 
            "image_exists": image_exists[page_num]
        })
        
        with open(text_output_path, "a", encoding="utf-8") as text_file:
            text_file.write(f"Page {page_num + 1}\n{'=' * 40}\n{text_data}\n\n")

    for page_data in all_page_data:
        insert_page_data(page_data["page_num"], page_data["text_data"], page_data["image_exists"])

def main():
    table_create()
    pdf_path = "sample2.pdf"
    text_output_path = "extracted_text.txt"
    images_output_folder = "extracted_images"

    
    extract_text_and_save_to_db(pdf_path, text_output_path, images_output_folder)

if __name__ == "__main__":
    main()
