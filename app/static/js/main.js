// Flash message auto-dismiss
document.addEventListener('DOMContentLoaded', function() {
    // Auto-hide flash messages after 5 seconds
    const flashMessages = document.querySelectorAll('.flash');
    flashMessages.forEach(flash => {
        setTimeout(() => {
            flash.style.opacity = '0';
            flash.style.transform = 'translateY(-10px)';
            setTimeout(() => flash.remove(), 300);
        }, 5000);
    });
});

// Form validation enhancement
document.addEventListener('DOMContentLoaded', function() {
    const registerForm = document.querySelector('.auth-form');
    if (registerForm && window.location.pathname.includes('register')) {
        registerForm.addEventListener('submit', function(e) {
            const password = document.getElementById('password');
            const confirmPassword = document.getElementById('confirm_password');
            
            if (password && confirmPassword) {
                if (password.value !== confirmPassword.value) {
                    e.preventDefault();
                    alert('Passwords do not match. Please try again.');
                    confirmPassword.focus();
                }
                
                if (password.value.length < 6) {
                    e.preventDefault();
                    alert('Password must be at least 6 characters long.');
                    password.focus();
                }
            }
        });
    }
});

// Smooth button interactions
document.addEventListener('click', function(e) {
    if (e.target.matches('.btn:not(.btn-disabled)')) {
        e.target.style.transform = 'scale(0.98)';
        setTimeout(() => {
            e.target.style.transform = '';
        }, 100);
    }
});

// Order confirmation animation
document.addEventListener('DOMContentLoaded', function() {
    const successIcon = document.querySelector('.success-icon');
    if (successIcon) {
        setTimeout(() => {
            successIcon.style.animation = 'bounce 0.5s ease';
        }, 100);
    }
});

// Add bounce animation
const style = document.createElement('style');
style.textContent = `
    @keyframes bounce {
        0%, 100% { transform: scale(1); }
        50% { transform: scale(1.1); }
    }
`;
document.head.appendChild(style);
