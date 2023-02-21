const modal = document.querySelector(".modal");
const btn_landing = document.querySelector(".landing_button1");
const span = document.querySelector(".close");
const overlay = document.querySelector(".overlay");

const openModal = function () {
  modal.classList.remove("hidden");
  overlay.classList.remove("hidden");
};

const closeModal = function (){
  modal.classList.add("hidden");
  overlay.classList.add("hidden");
}

btn_landing.addEventListener("click", openModal);
span.addEventListener("click", closeModal);



const btn1 =document.querySelector(".description");
const description1=document.querySelector(".modal_description");
const span2 = document.querySelector(".close_desc");
const descriptions = document.querySelectorAll(".description_content")

const closeDesciption = function () {
  description1.classList.add("hidden");
  overlay.classList.add("hidden");
  descriptions.forEach(c => c.classList.add('hidden'));
};

span2.addEventListener("click", closeDesciption);

function show_desc(clicked_id)
{
    description1.classList.remove("hidden");
    overlay.classList.remove("hidden");
    document
      .querySelector(`.description_content--${clicked_id}`)
      .classList.remove('hidden');
}
