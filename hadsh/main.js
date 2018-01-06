
/* Window state */
var page = 1;
var textbox = null;

var getNextPage = function() {
	var rq = new XMLHttpRequest();
	rq.onreadystatechange = function() {
		if (this.readyState == 4 && this.status == 200) {
			// Typical action to be performed when
			// the document is ready:

			var data = JSON.parse(rq.responseText);
			data.users.forEach(function (user) {
				var userBox = document.createElement('div');
				var avatar = document.createElement('img');
				avatar.src = '/avatar/' + user.avatar_id;
				avatar.class = 'avatar';
				userBox.appendChild(avatar);
				var profile_link = document.createElement('a');
				profile_link.href = user.url;
				var profile_name = document.createElement('tt');
				profile_name.innerHTML = user.screen_name;
				profile_link.appendChild(profile_name);
				userBox.appendChild(profile_link);
				var links = document.createElement('ul');
				user.links.forEach(function (link) {
					var link_tag = document.createElement('a');
					link_tag.href = link.url;
					link_tag.title = link.title;
					links.appendChild(link_tag);
				});
				userBox.appendChild(links);
				textbox.appendChild(userBox);
			});
			textbox.appendChild(document.createElement('hr'));
			page++;
		}
		};
	rq.open("GET", "/data/newcomers.json?page=" + page, true);
	rq.send();
};

var main = function() {
	window.onscroll = function(ev) {
		if ((window.innerHeight + window.scrollY)
			>= document.body.offsetHeight) {
			getNextPage();
		}
	};

	textbox = document.getElementById('recent');
	getNextPage();
};
