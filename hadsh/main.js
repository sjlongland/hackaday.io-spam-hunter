
/* Window state */
var page = 1;
var textbox = null;
var busy = false;

var getNextPage = function() {
	var rq = new XMLHttpRequest();
	busy = true;
	var loading_msg = document.createElement('h3');
	loading_msg.innerHTML = 'Loading...';
	textbox.appendChild(loading_msg);

	rq.onreadystatechange = function() {
		if (this.readyState == 4) {
			// Typical action to be performed when
			// the document is ready:
			textbox.removeChild(loading_msg);

			if (this.status === 200) {
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
						link_tag.innerHTML = link.title;
						var link_item = document.createElement('li');
						link_item.appendChild(link_tag);
						links.appendChild(link_item);
					});
					userBox.appendChild(links);
					textbox.appendChild(userBox);
				});
				textbox.appendChild(document.createElement('hr'));
			}
			page++;
			busy = false;
		}
		};
	rq.open("GET", "/data/newcomers.json?page=" + page, true);
	rq.send();
};

var main = function() {
	window.onscroll = function(ev) {
		if ((window.innerHeight + window.scrollY)
			>= document.body.offsetHeight) {
			if (!busy)
				getNextPage();
		}
	};

	textbox = document.getElementById('recent');
	getNextPage();
};
