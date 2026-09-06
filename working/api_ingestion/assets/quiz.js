/* Quiz widget component - reusable across lessons */
function createQuiz(containerId, question, options, correctIndex, explanation) {
  const container = document.getElementById(containerId);
  if (!container) return;
  
  container.className = 'quiz';
  
  const qDiv = document.createElement('div');
  qDiv.className = 'quiz-question';
  qDiv.textContent = question;
  container.appendChild(qDiv);
  
  const ul = document.createElement('ul');
  ul.className = 'quiz-options';
  
  options.forEach((opt, i) => {
    const li = document.createElement('li');
    li.textContent = opt;
    li.addEventListener('click', () => {
      // Disable all options
      ul.querySelectorAll('li').forEach(el => el.classList.add('disabled'));
      
      if (i === correctIndex) {
        li.classList.add('correct');
        feedback.innerHTML = '✅ ' + explanation;
      } else {
        li.classList.add('wrong');
        ul.children[correctIndex].classList.add('correct');
        feedback.innerHTML = '❌ ' + explanation;
      }
      feedback.style.display = 'block';
    });
    ul.appendChild(li);
  });
  container.appendChild(ul);
  
  const feedback = document.createElement('div');
  feedback.className = 'quiz-feedback';
  container.appendChild(feedback);
}
