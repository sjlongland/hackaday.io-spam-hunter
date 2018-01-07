
/* Window state */
var page = 1;
var textbox = null;
var busy = false;

var getNextPage = function() {
	var rq = new XMLHttpRequest();
	busy = true;
	var loading_msg = document.createElement('pre');
	var spinner = '-';
	var dots = '';
	textbox.appendChild(loading_msg);

	var nextSpinner = function() {
		if (busy) {
			window.setTimeout(nextSpinner, 250);
		}

		switch (spinner) {
		case '-':	spinner = '\\';	break;
		case '\\':	spinner = '|'; break;
		case '|':	spinner = '/'; break;
		default:
				spinner = '-';
				dots += '.';
				break;
		}

		loading_msg.innerHTML = 'Loading' + dots + spinner;
	};
	nextSpinner();

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
					avatar.src = '/avatar/' + user.avatar_id
								+ '?width=100&height=100';
					avatar.class = 'avatar';
					userBox.appendChild(avatar);

					var profile_link = document.createElement('a');
					profile_link.href = user.url;
					var profile_name = document.createElement('tt');
					profile_name.innerHTML = user.screen_name;
					profile_link.appendChild(profile_name);
					userBox.appendChild(profile_link);

					if (user.location) {
						var profile_location = document.createElement('div');
						profile_location.innerHTML = user.location;
						userBox.appendChild(profile_location);
					}

					if (user.about_me) {
						var profile_about_me = document.createElement('div');
						profile_about_me.innerHTML = user.about_me;
						userBox.appendChild(profile_about_me);
					}

					if (user.who_am_i) {
						var profile_who_am_i = document.createElement('div');
						profile_about_me.innerHTML = user.who_am_i;
						userBox.appendChild(profile_who_am_i);
					}

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
				page++;
			}
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
