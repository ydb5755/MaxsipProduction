
var state = {
    'requested-date':getDate(0),
    'days-in-past':0,
    'date-lower-limit':getDateLowerLimit()
  }
  
const rightArrow = document.getElementById('date-arrow-right');
const leftArrow = document.getElementById('date-arrow-left');
const showMoreButton = document.getElementById('show-more');
const agentRadio = document.getElementById('agent-radio');
const masterAgentRadio = document.getElementById('master-agent-radio');
const tableBody = document.getElementById('table-body');
const topLeftTitle = document.getElementById('top-left-title');
const totalTarget = document.getElementById('total-active-target');
const totalAgentsTarget = document.getElementById('total-agent-target');

async function getDateLowerLimit(){
  const result = await fetch('/agent_reports/get_date_lower_limit_for_customer_age_analysis');
  const data = await result.json();
  state['date-lower-limit'] = data.result
}

function getDate(days_in_past){
  var today = new Date();
  let requested_day = new Date(today.setDate(today.getDate()-days_in_past));
  var date = requested_day.toLocaleDateString('en-US', {
    year: 'numeric',
    month: 'long',
    day: 'numeric'
  });
  return date;
  }

function setDate(days_in_past){
  state['requested-date'] = getDate(days_in_past);
  var target = document.getElementById('current-date-target');
  target.innerText = state['requested-date'];
}
  
async function getTotal(agent_or_master_agent, day_request_offset){
  const result = await fetch(`/agent_reports/get_total_customer_count_for_customer_age_analysis_by_agent_and_master_agent/${agent_or_master_agent}/${day_request_offset}`);
  const data = await result.json();
  totalTarget.innerText = data.count.toLocaleString();
}
async function getTotalAgents(agent_or_master_agent, day_request_offset){
  const result = await fetch(`/agent_reports/get_total_number_of_agents_for_customer_age_analysis/${agent_or_master_agent}/${day_request_offset}`);
  const data = await result.json();
  totalAgentsTarget.innerText = data.count.toLocaleString();
}
async function getFiveAgents(offset, agent_or_master_agent, day_request_offset){
  var table = document.getElementById('table-body');
  rightArrow.classList = '';
  leftArrow.classList = '';
  const result = await fetch(`/agent_reports/get_all_agents_for_customer_age_analysis_by_agent_and_master_agent/${offset}/${agent_or_master_agent}/${day_request_offset}`);
  const data = await result.json();
  var agents = data.agents
  for (let i = 0; i < agents.length; i++) {
    var row = document.createElement('tr');
    for (let j = 0; j < agents[i].length; j++){
        var td = document.createElement('td');
        td.classList = 'text-center';
        var container = document.createElement('div');
        container.classList = 'nums-and-arrows';
        var numberContainer = document.createElement('div');
        numberContainer.classList = 'child';
        container.appendChild(numberContainer);
        numberContainer.innerText = agents[i][j].toLocaleString();
        row.appendChild(td);
        td.appendChild(container);
    }
    table.appendChild(row);
  }
  if (state['days-in-past'] != 0){
    rightArrow.classList = 'fa-solid fa-arrow-right fa-xl';
  }
  if (state['days-in-past'] != state['date-lower-limit']){
    leftArrow.className = 'fa-solid fa-arrow-left fa-xl';
  }
  getTotal(agent_or_master_agent, day_request_offset);
  getTotalAgents(agent_or_master_agent, day_request_offset);
}

document.addEventListener('DOMContentLoaded', async () => {
  setDate(0);
  getFiveAgents(0, 'agent', 0);
});

rightArrow.addEventListener('click', () => {
  state['days-in-past']--;
  state['requested-date'] = getDate(state['days-in-past']);
  setDate(state['days-in-past']);
  tableBody.innerHTML = '';
  if (agentRadio.checked){
    getFiveAgents(0, 'agent', state['days-in-past']);
  } else {
    getFiveAgents(0, 'master_agent', state['days-in-past']);
  }

});

leftArrow.addEventListener('click', () => {
  state['days-in-past']++;
  state['requested-date'] = getDate(state['days-in-past']);
  setDate(state['days-in-past']);
  tableBody.innerHTML = '';
  if (agentRadio.checked){
    getFiveAgents(0, 'agent', state['days-in-past']);
  } else {
    getFiveAgents(0, 'master_agent', state['days-in-past']);
  }
})

showMoreButton.addEventListener('click', () => {
  const offset = tableBody.childElementCount;
  if (agentRadio.checked){
    getFiveAgents(offset, 'agent', state['days-in-past']);
  } else {
    getFiveAgents(offset, 'master_agent', state['days-in-past']);
  }
});

agentRadio.addEventListener('change', () => {
  tableBody.innerHTML = '';
  getFiveAgents(0, 'agent', state['days-in-past']);
  topLeftTitle.innerText = 'Agent Information'
});

masterAgentRadio.addEventListener('change', () => {
  tableBody.innerHTML = '';
  getFiveAgents(0, 'master_agent', state['days-in-past']);
  topLeftTitle.innerText = 'Master Agent Information'
});
  