document.addEventListener("DOMContentLoaded", function () {
  const submenuToggles = document.querySelectorAll(".submenu-toggle");





  submenuToggles.forEach((toggle, index) => {
    const parentLi = toggle.closest(".has-submenu");

    // Create a unique key for each toggle
    const key = toggle.textContent.trim();

    // Restore state from localStorage
    const isOpen = localStorage.getItem(key) === "true";
    if (isOpen) {
      parentLi.classList.add("open");
      toggle.innerHTML = key + " ▼";
    }



    // Toggle submenu on click
    toggle.addEventListener("click", function (e) {
      e.preventDefault();

      const currentlyOpen = parentLi.classList.contains("open");
      parentLi.classList.toggle("open");

      // Store new state
      localStorage.setItem(key, !currentlyOpen);

      // Update arrow
      this.innerHTML = !currentlyOpen ? key + " ▼" : key + " ▸";
    });
  });
});
