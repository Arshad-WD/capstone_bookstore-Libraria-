"""
Database initialization script for BookBazaar.
Creates tables and seeds with extensive book data (INR Pricing) and admin user.
"""

from app import create_app
from app.extensions import db
from app.models.book import Book
from app.models.user import User

def init_database():
    """Initialize database and seed with sample data."""
    
    app = create_app()
    
    with app.app_context():
        # Drop all tables and recreate (for clean initialization)
        print("Dropping existing tables...")
        db.drop_all()
        
        print("Creating new tables...")
        db.create_all()
        
        # Seed books with extensive realistic data (INR Prices)
        print("Seeding books...")
        
        books = [
            # Fiction - Bestsellers
            Book(
                title="The Midnight Library",
                author="Matt Haig",
                description="Between life and death there is a library. When Nora Seed finds herself in the Midnight Library, she has a chance to make things right.",
                price=499.00,
                stock=15,
                image_url="https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=400"
            ),
            Book(
                title="Project Hail Mary",
                author="Andy Weir",
                description="A lone astronaut must save the earth from disaster in this incredible new science-based thriller from the author of The Martian.",
                price=599.00,
                stock=8,
                image_url="https://images.unsplash.com/photo-1614544048536-0d28caf77f42?w=400"
            ),
            Book(
                title="The Seven Husbands of Evelyn Hugo",
                author="Taylor Jenkins Reid",
                description="Aging and reclusive Hollywood movie icon Evelyn Hugo is finally ready to tell the truth about her glamorous and scandalous life.",
                price=450.00,
                stock=16,
                image_url="https://images.unsplash.com/photo-1543002588-bfa74002ed7e?w=400"
            ),
            Book(
                title="The Silent Patient",
                author="Alex Michaelides",
                description="A woman's act of violence against her husband—and the therapist obsessed with uncovering her motive. A shocking psychological thriller.",
                price=399.00,
                stock=13,
                image_url="https://images.unsplash.com/photo-1541963463532-d68292c34b19?w=400"
            ),
            Book(
                title="Klara and the Sun",
                author="Kazuo Ishiguro",
                description="From her place in the store, Klara, an Artificial Friend with outstanding observational qualities, watches carefully the behavior of those who come in.",
                price=550.00,
                stock=9,
                image_url="https://images.unsplash.com/photo-1519682337058-a94d519337bc?w=400"
            ),
            Book(
                title="The Four Winds",
                author="Kristin Hannah",
                description="A stunning novel about the bonds of family and the power of hope. Texas, 1921. The Great Depression looms on the horizon.",
                price=495.00,
                stock=7,
                image_url="https://images.unsplash.com/photo-1524578271613-d550eacf6090?w=400"
            ),
            Book(
                title="Where the Crawdads Sing",
                author="Delia Owens",
                description="For years, rumors of the 'Marsh Girl' have haunted Barkley Cove. So in late 1969, when handsome Chase Andrews is found dead, the locals immediately suspect her.",
                price=425.00,
                stock=20,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            Book(
                title="The Invisible Life of Addie LaRue",
                author="V.E. Schwab",
                description="A life no one will remember. A story you will never forget. France, 1714: in a moment of desperation, a young woman makes a Faustian bargain.",
                price=525.00,
                stock=11,
                image_url="https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=400"
            ),
            Book(
                title="The House in the Cerulean Sea",
                author="T.J. Klune",
                description="A magical island. A dangerous task. A burning secret. Linus Baker leads a quiet, solitary life. At forty, he lives in a tiny house with a devious cat.",
                price=480.00,
                stock=14,
                image_url="https://images.unsplash.com/photo-1516979187457-637abb4f9353?w=400"
            ),
            Book(
                title="Anxious People",
                author="Fredrik Backman",
                description="A poignant comedy about a crime that never happened, a hostage drama that never unfolded, and eight extremely anxious strangers.",
                price=395.00,
                stock=12,
                image_url="https://images.unsplash.com/photo-1476275466078-4007374efbbe?w=400"
            ),
            
            # Science Fiction & Fantasy
            Book(
                title="Dune",
                author="Frank Herbert",
                description="Set on the desert planet Arrakis, Dune is the story of the boy Paul Atreides, heir to a noble family tasked with ruling this inhospitable world.",
                price=699.00,
                stock=18,
                image_url="https://images.unsplash.com/photo-1621351183012-e2f9972dd9bf?w=400"
            ),
            Book(
                title="The Name of the Wind",
                author="Patrick Rothfuss",
                description="Told in Kvothe's own voice, this is the tale of the magically gifted young man who grows to be the most notorious wizard his world has ever seen.",
                price=550.00,
                stock=14,
                image_url="https://images.unsplash.com/photo-1535905557558-afc4877a26fc?w=400"
            ),
            Book(
                title="The Way of Kings",
                author="Brandon Sanderson",
                description="Epic fantasy series starter. Roshar is a world of stone and storms. Unite them. A breathtaking saga of war and magic.",
                price=899.00,
                stock=10,
                image_url="https://images.unsplash.com/photo-1618168912690-2afb80f5f9a1?w=400"
            ),
            Book(
                title="Neuromancer",
                author="William Gibson",
                description="The groundbreaking cyberpunk novel. Case was the sharpest data-thief in the matrix—until he crossed the wrong people.",
                price=399.00,
                stock=12,
                image_url="https://images.unsplash.com/photo-1526374965328-7f61d4dc18c5?w=400"
            ),
            Book(
                title="Foundation",
                author="Isaac Asimov",
                description="The first novel in the Foundation series. Galactic Empire is crumbling. Haria Seldon uses psychohistory to save knowledge.",
                price=349.00,
                stock=15,
                image_url="https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=400"
            ),
            Book(
                title="The Fellowship of the Ring",
                author="J.R.R. Tolkien",
                description="The first volume in the Lord of the Rings trilogy. One Ring to rule them all, One Ring to find them, One Ring to bring them all.",
                price=599.00,
                stock=22,
                image_url="https://images.unsplash.com/photo-1621351183012-e2f9972dd9bf?w=400"
            ),
            Book(
                title="American Gods",
                author="Neil Gaiman",
                description="A strange and unsettling story of the battle between the old gods and the new ones. Shadow Moon gets caught in the middle.",
                price=499.00,
                stock=10,
                image_url="https://images.unsplash.com/photo-1506466010722-395ee2bef877?w=400"
            ),
            
            # Non-Fiction - Self-Help & Business
            Book(
                title="Atomic Habits",
                author="James Clear",
                description="An Easy & Proven Way to Build Good Habits & Break Bad Ones. Transform your life with tiny changes that deliver remarkable results.",
                price=550.00,
                stock=25,
                image_url="https://images.unsplash.com/photo-1589829085413-56de8ae18c73?w=400"
            ),
            Book(
                title="The Psychology of Money",
                author="Morgan Housel",
                description="Timeless lessons on wealth, greed, and happiness. Doing well with money has little to do with how smart you are and a lot to do with how you behave.",
                price=350.00,
                stock=22,
                image_url="https://images.unsplash.com/photo-1633158829585-23ba8f7c8caf?w=400"
            ),
            Book(
                title="Thinking, Fast and Slow",
                author="Daniel Kahneman",
                description="The definitive book on behavioral economics and cognitive biases. A landmark work that explores the two systems that drive the way we think.",
                price=750.00,
                stock=11,
                image_url="https://images.unsplash.com/photo-1532012197267-da84d127e765?w=400"
            ),
            Book(
                title="Deep Work",
                author="Cal Newport",
                description="Rules for Focused Success in a Distracted World. Learn how to master the art of deep work and dramatically improve your productivity.",
                price=450.00,
                stock=17,
                image_url="https://images.unsplash.com/photo-1484480974693-6ca0a78fb36b?w=400"
            ),
            Book(
                title="The Lean Startup",
                author="Eric Ries",
                description="How Today's Entrepreneurs Use Continuous Innovation to Create Radically Successful Businesses. A must-read for entrepreneurs.",
                price=599.00,
                stock=13,
                image_url="https://images.unsplash.com/photo-1553877522-43269d4ea984?w=400"
            ),
            Book(
                title="Start With Why",
                author="Simon Sinek",
                description="How Great Leaders Inspire Everyone to Take Action. Discover the power of WHY in business and life.",
                price=499.00,
                stock=19,
                image_url="https://images.unsplash.com/photo-1519389950473-47ba0277781c?w=400"
            ),
            Book(
                title="Shoe Dog",
                author="Phil Knight",
                description="A Memoir by the Creator of Nike. An honest and riveting look at the building of a global brand.",
                price=550.00,
                stock=15,
                image_url="https://images.unsplash.com/photo-1542291026-7eec264c27ff?w=400"
            ),
            Book(
                title="Quiet",
                author="Susan Cain",
                description="The Power of Introverts in a World That Can't Stop Talking. A book that changed the conversation about introverts.",
                price=399.00,
                stock=20,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            
            # Non-Fiction - History & Science
            Book(
                title="Sapiens",
                author="Yuval Noah Harari",
                description="A Brief History of Humankind. How did our species succeed in the battle for dominance? Why did our foraging ancestors come together to create cities?",
                price=599.00,
                stock=14,
                image_url="https://images.unsplash.com/photo-1550399105-c4db5fb85c18?w=400"
            ),
            Book(
                title="Educated",
                author="Tara Westover",
                description="A Memoir. An unforgettable story of a young woman who leaves her survivalist family and goes on to earn a PhD from Cambridge.",
                price=450.00,
                stock=16,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            Book(
                title="Homo Deus",
                author="Yuval Noah Harari",
                description="A Brief History of Tomorrow. What will happen to us when artificial intelligence outperforms humans? The sequel to Sapiens.",
                price=625.00,
                stock=10,
                image_url="https://images.unsplash.com/photo-1451187580459-43490279c0fa?w=400"
            ),
            Book(
                title="A Short History of Nearly Everything",
                author="Bill Bryson",
                description="A humorous and accessible history of science, covering everything from the Big Bang to the rise of civilization.",
                price=450.00,
                stock=9,
                image_url="https://images.unsplash.com/photo-1446776877081-d282a0f896e2?w=400"
            ),
            Book(
                title="The Immortal Life of Henrietta Lacks",
                author="Rebecca Skloot",
                description="The story of a poor Southern tobacco farmer whose cancer cells became one of the most important tools in medicine.",
                price=399.00,
                stock=12,
                image_url="https://images.unsplash.com/photo-1532187863486-abf9dbad1b69?w=400"
            ),
            Book(
                title="Guns, Germs, and Steel",
                author="Jared Diamond",
                description="The Fates of Human Societies. Why did some civilizations thrive while others languished? A Pulitzer Prize-winning classic.",
                price=499.00,
                stock=8,
                image_url="https://images.unsplash.com/photo-1535905557558-afc4877a26fc?w=400"
            ),
            Book(
                title="The Gene",
                author="Siddhartha Mukherjee",
                description="An Intimate History. The story of the gene—the fundamental unit of heredity and the building block of life.",
                price=850.00,
                stock=11,
                image_url="https://images.unsplash.com/photo-1530633173740-195931707324?w=400"
            ),
            
            # Mystery & Thriller
            Book(
                title="Gone Girl",
                author="Gillian Flynn",
                description="On the morning of his fifth wedding anniversary, Nick's wife Amy suddenly disappears. The evidence suggests foul play.",
                price=399.00,
                stock=15,
                image_url="https://images.unsplash.com/photo-1543002588-bfa74002ed7e?w=400"
            ),
            Book(
                title="The Girl with the Dragon Tattoo",
                author="Stieg Larsson",
                description="Murder mystery, family saga, love story, and financial intrigue combine into one satisfyingly complex thriller.",
                price=450.00,
                stock=11,
                image_url="https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=400"
            ),
            Book(
                title="The Da Vinci Code",
                author="Dan Brown",
                description="A murder in the Louvre and clues in Da Vinci paintings lead to the discovery of a religious mystery protected by a secret society.",
                price=350.00,
                stock=18,
                image_url="https://images.unsplash.com/photo-1519682337058-a94d519337bc?w=400"
            ),
            Book(
                title="Big Little Lies",
                author="Liane Moriarty",
                description="A murder, a tragic accident, or just good parents gone bad? A story of secrets and lies in a small coastal town.",
                price=425.00,
                stock=14,
                image_url="https://images.unsplash.com/photo-1541963463532-d68292c34b19?w=400"
            ),
            Book(
                title="Sharp Objects",
                author="Gillian Flynn",
                description="Camille Preaker returns to her hometown to report on a string of murders. She must face her own past.",
                price=380.00,
                stock=10,
                image_url="https://images.unsplash.com/photo-1543002588-bfa74002ed7e?w=400"
            ),
            
            # Classic Literature
            Book(
                title="1984",
                author="George Orwell",
                description="A dystopian social science fiction novel. Big Brother is watching. A must-read classic about totalitarianism and surveillance.",
                price=299.00,
                stock=20,
                image_url="https://images.unsplash.com/photo-1541963463532-d68292c34b19?w=400"
            ),
            Book(
                title="To Kill a Mockingbird",
                author="Harper Lee",
                description="The unforgettable novel of a childhood in a sleepy Southern town and the crisis of conscience that rocked it.",
                price=250.00,
                stock=22,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            Book(
                title="Pride and Prejudice",
                author="Jane Austen",
                description="A romantic novel of manners about the perils of misconstrued first impressions. A timeless classic.",
                price=199.00,
                stock=17,
                image_url="https://images.unsplash.com/photo-1520451644838-906a72aa7c86?w=400"
            ),
            Book(
                title="The Great Gatsby",
                author="F. Scott Fitzgerald",
                description="The story of the mysteriously wealthy Jay Gatsby and his love for the beautiful Daisy Buchanan. An American classic.",
                price=225.00,
                stock=19,
                image_url="https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=400"
            ),
            Book(
                title="Brave New World",
                author="Aldous Huxley",
                description="A chilling vision of a future world where happiness is mandatory and individuality is suppressed.",
                price=280.00,
                stock=13,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            Book(
                title="The Catcher in the Rye",
                author="J.D. Salinger",
                description="The ultimate novel of teenage angst and rebellion. Holden Caulfield's journey through New York City.",
                price=249.00,
                stock=16,
                image_url="https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=400"
            ),
            
            # Young Adult
            Book(
                title="The Hunger Games",
                author="Suzanne Collins",
                description="In a dystopian future, teenagers are forced to fight to the death in a televised spectacle. A gripping dystopian trilogy starter.",
                price=349.00,
                stock=24,
                image_url="https://images.unsplash.com/photo-1535905557558-afc4877a26fc?w=400"
            ),
            Book(
                title="Harry Potter and the Sorcerer's Stone",
                author="J.K. Rowling",
                description="The magical beginning of Harry Potter's journey. A boy discovers he's a wizard on his 11th birthday.",
                price=499.00,
                stock=30,
                image_url="https://images.unsplash.com/photo-1621351183012-e2f9972dd9bf?w=400"
            ),
            Book(
                title="The Fault in Our Stars",
                author="John Green",
                description="A love story about two teens with cancer. Funny, raw, and honest. An emotional journey you won't forget.",
                price=325.00,
                stock=14,
                image_url="https://images.unsplash.com/photo-1524578271613-d550eacf6090?w=400"
            ),
            Book(
                title="The Book Thief",
                author="Markus Zusak",
                description="Narrated by Death, this is the story of Liesel Meminger, a young girl living in Nazi Germany.",
                price=450.00,
                stock=12,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            Book(
                title="Wonder",
                author="R.J. Palacio",
                description="The story of August Pullman, a boy born with facial differences who enters a mainstream school for the first time.",
                price=299.00,
                stock=18,
                image_url="https://images.unsplash.com/photo-1544947950-fa07a98d237f?w=400"
            ),
            
            # Biography & Memoir
            Book(
                title="Becoming",
                author="Michelle Obama",
                description="An intimate, powerful, and inspiring memoir by the former First Lady of the United States.",
                price=650.00,
                stock=16,
                image_url="https://images.unsplash.com/photo-1512820790803-83ca734da794?w=400"
            ),
            Book(
                title="Steve Jobs",
                author="Walter Isaacson",
                description="The exclusive biography of Steve Jobs. Based on more than forty interviews with Jobs over two years.",
                price=799.00,
                stock=10,
                image_url="https://images.unsplash.com/photo-1519389950473-47ba0277781c?w=400"
            ),
            Book(
                title="Long Walk to Freedom",
                author="Nelson Mandela",
                description="The autobiography of Nelson Mandela. The riveting memoirs of the man who rose from tribal chief to president of South Africa.",
                price=550.00,
                stock=8,
                image_url="https://images.unsplash.com/photo-1524578271613-d550eacf6090?w=400"
            ),
            Book(
                title="Born a Crime",
                author="Trevor Noah",
                description="Stories from a South African Childhood. A compelling memoir about growing up during and after apartheid.",
                price=495.00,
                stock=15,
                image_url="https://images.unsplash.com/photo-1544652478-665ca89b9b4a?w=400"
            ),
            Book(
                title="The Glass Castle",
                author="Jeannette Walls",
                description="A remarkable memoir of resilience and redemption, and a revelatory look into a family at once deeply dysfunctional and uniquely vibrant.",
                price=399.00,
                stock=13,
                image_url="https://images.unsplash.com/photo-1524578271613-d550eacf6090?w=400"
            ),
        ]
        
        for book in books:
            db.session.add(book)
        
        db.session.commit()
        print(f"✓ Added {len(books)} books to database")
        
        # Create demo buyer user
        print("Creating demo buyer user...")
        demo_user = User(
            username="demo",
            email="demo@bookbazaar.com",
            role="buyer"
        )
        demo_user.set_password("demo123")
        db.session.add(demo_user)
        
        # Create demo seller user
        print("Creating demo seller user...")
        seller_user = User(
            username="seller",
            email="seller@bookbazaar.com",
            role="seller",
            is_validated=True
        )
        seller_user.set_password("seller123")
        db.session.add(seller_user)
        
        # Create admin user
        print("Creating admin user...")
        admin_user = User(
            username="admin",
            email="admin@bookbazaar.com",
            role="admin",
            is_validated=True
        )
        admin_user.set_password("admin123")
        db.session.add(admin_user)
        
        db.session.commit()
        print("✓ Created demo buyer (email: demo@bookbazaar.com, password: demo123)")
        print("✓ Created demo seller (email: seller@bookbazaar.com, password: seller123)")
        print("✓ Created admin user (email: admin@bookbazaar.com, password: admin123)")
        
        print("\n" + "="*50)
        print("Database initialized successfully!")
        print("="*50)
        print(f"\n📚 Total Books: {len(books)}")
        print("\nAccounts created:")
        print("  Buyer:    demo@bookbazaar.com / demo123")
        print("  Seller:   seller@bookbazaar.com / seller123")
        print("  Admin:    admin@bookbazaar.com / admin123")
        print("\nRun the application with: python run.py")

if __name__ == "__main__":
    init_database()
