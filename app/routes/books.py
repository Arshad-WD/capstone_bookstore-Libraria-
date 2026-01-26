from flask import Blueprint, render_template, request, redirect
from app.repositories.book_repo import BookRepository
from app.models.book import Book

books_bp = Blueprint("books", __name__)
repo = BookRepository()

@books_bp.route("/books", methods=["GET", "POST"])
def books():
    if request.method == "POST":
        book = Book(
            title=request.form["title"],
            author=request.form["author"],
            price=float(request.form["price"]),
            stock=int(request.form["stock"])
        )
        repo.add(book)
        return redirect("/books")
    
    return render_template("books.html", books=repo.get_all())