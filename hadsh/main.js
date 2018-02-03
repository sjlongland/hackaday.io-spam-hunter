
/* Window state */
var page = 1;
var textbox = null;
var busy = false;

var auto_mark = {};
var mass_mark_legit = [];
var mass_mark_btn = null;

var newest_uid = null;
var oldest_uid = null;

/* Credit: https://stackoverflow.com/a/7124052 */
var htmlEscape = function(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
};

var getNextPage = function() {
	var rq = new XMLHttpRequest();
	busy = true;
	var loading_msg = document.createElement('pre');
	var spinner = '-';
	var dots = '';
	textbox.appendChild(loading_msg);
	mass_mark_legit = Object.keys(auto_mark);

	if (mass_mark_btn !== null) {
		textbox.removeChild(mass_mark_btn);
	}

	if (mass_mark_legit.length > 0) {
		mass_mark_btn = document.createElement('button');
		mass_mark_btn.innerHTML = ('Mark above '
			+ mass_mark_legit.length
			+ ' auto_legit accounts as legit');
		mass_mark_btn.onclick = function() {
			mass_mark_legit.forEach(function (uid) {
				auto_mark[uid](true);
			});
			textbox.removeChild(mass_mark_btn);
			mass_mark_btn = null;
			window.scrollTo(0,0);
		};
		textbox.appendChild(mass_mark_btn);
	}

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
		try {
			if (this.readyState == 4) {
				// Typical action to be performed when
				// the document is ready:
				textbox.removeChild(loading_msg);

				if (this.status === 200) {
					var data = JSON.parse(rq.responseText);
					data.users.forEach(function (user) {
						if ((newest_uid === null)
							|| (newest_uid < user.id))
							newest_uid = user.id;

						if ((oldest_uid === null)
							|| (oldest_uid > user.id))
							oldest_uid = user.id;

						var userBox = document.createElement('div');
						var avatarBox = document.createElement('div');
						var avatar = document.createElement('img');
						avatar.src = '/avatar/' + user.avatar_id
							+ '?width=100&height=100';
						avatarBox.width = 100;
						avatarBox.height = 100;
						avatarBox.appendChild(avatar);
						userBox.appendChild(avatarBox);

						var profile_link = document.createElement('a');
						profile_link.href = user.url;
						var profile_name = document.createElement('tt');
						profile_name.innerHTML = user.screen_name;
						profile_link.appendChild(profile_name);
						userBox.appendChild(profile_link);

						var profile_uid = document.createTextNode(' ' + user.id);
						userBox.appendChild(profile_uid);

						var profile_score = document.createElement('div');
						userBox.appendChild(profile_score);

						var profile_created = document.createElement('div');
						profile_created.innerHTML = user.created;
						userBox.appendChild(profile_created);

						var profile_groups = document.createElement('div');
						var group_set = {};
						user.groups.forEach(function (group) {
							var group_label = document.createElement('tt');
							group_label.innerHTML = group;
							profile_groups.appendChild(group_label);
							group_set[group] = true;
						});

						var rm_auto = function() {
							if (auto_mark[user.id] !== undefined) {
								delete auto_mark[user.id];
							}
						};

						if (!group_set.legit) {
							var classify_legit = document.createElement('button');
							classify_legit.innerHTML = 'Legit';
							var do_classify = function(mass_update) {
								var rq = new XMLHttpRequest();
								rm_auto();
								rq.open('POST', '/classify/' + user.id);
								rq.setRequestHeader("Content-type", 'application/json');
								rq.send(JSON.stringify("legit"));
								rq.onreadystatechange = function() {
									if ((this.readyState === 4) && (this.status === 200)) {
										setTimeout(function () {
											textbox.removeChild(userBox);
										}, ((mass_update === true) ? 500 : 10000));
									}
								};
								profile_groups.removeChild(classify_legit);
							};
							if (group_set.auto_legit) {
								auto_mark[user.id] = do_classify;
							}

							classify_legit.onclick = do_classify;
							profile_groups.appendChild(classify_legit);
						}
						if (!group_set.suspect) {
							var classify_suspect = document.createElement('button');
							classify_suspect.innerHTML = 'Suspect';
							classify_suspect.onclick = function() {
								var rq = new XMLHttpRequest();
								rm_auto();
								rq.open('POST', '/classify/' + user.id);
								rq.setRequestHeader("Content-type", 'application/json');
								rq.send(JSON.stringify("suspect"));
								rq.onreadystatechange = function() {
									if ((this.readyState === 4) && (this.status === 200)) {
										setTimeout(function () {
											textbox.removeChild(userBox);
										}, 10000);
									}
								};
								profile_groups.removeChild(classify_suspect);
							};
							profile_groups.appendChild(classify_suspect);
						}
						userBox.appendChild(profile_groups);

						var profile_tags = document.createElement('div');
						user.tags.forEach(function (tag) {
							var tag_label = document.createElement('tt');
							tag_label.innerHTML = tag;
							profile_tags.appendChild(tag_label);
						});
						userBox.appendChild(profile_tags);

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
							profile_who_am_i.innerHTML = user.who_am_i;
							userBox.appendChild(profile_who_am_i);
						}

						if (user.projects) {
							var profile_projects = document.createElement('div');
							profile_projects.innerHTML = user.projects + ' project(s)';
							userBox.appendChild(profile_projects);
						}

						if (user.tokens && Object.keys(user.tokens).length) {
							var profile_tokens = document.createElement('ul');
							Object.keys(user.tokens).forEach(function (token) {
								var token_li = document.createElement('li');
								var token_tt = document.createElement('tt');
								token_tt.innerHTML = htmlEscape(token);
								token_li.appendChild(token_tt);
								token_li.appendChild(document.createTextNode(' ' + user.tokens[token] + ' instances'));
								profile_tokens.appendChild(token_li);
							});
							userBox.appendChild(profile_tokens);
						}

						/* Compute the user's score */
						var user_score = 0.0;
						if (user.words && Object.keys(user.words).length) {
							var profile_words = document.createElement('div');
							Object.keys(user.words).forEach(function (word) {
								var stat = user.words[word];
								var word_span = document.createElement('span');
								var word_tt = document.createElement('tt');
								var score = 0.0;
								if (stat.site_count > 0) {
									score = stat.site_score / stat.site_count;
									user_score += score;
								}

								word_tt.innerHTML = htmlEscape(word);
								word_span.appendChild(word_tt);
								word_span.appendChild(
									document.createTextNode(
										' (' + stat.user_count
										+ ' occurances; score: '
										+ score
										+ ')'));
								profile_words.appendChild(word_span);
							});
							userBox.appendChild(profile_words);
						}

						if (user.word_adj && user.word_adj.length) {
							var profile_word_adj = document.createElement('div');
							user.word_adj.forEach(function (word_adj) {
								var adj_span = document.createElement('span');
								var adj_tt = document.createElement('tt');
								var score = 0.0;
								if (word_adj.site_count > 0) {
									score = word_adj.site_score / word_adj.site_count;
									user_score += score;
								}

								adj_tt.innerHTML = htmlEscape(word_adj.proceeding)
										+ ' &rarr; '
										+ htmlEscape(word_adj.following);
								adj_span.appendChild(adj_tt);
								adj_span.appendChild(
									document.createTextNode(
										' (' + word_adj.user_count
										+ ' occurances; score: '
										+ score
										+ ')'));
								profile_word_adj.appendChild(adj_span);

							});
							userBox.appendChild(profile_word_adj);
						}

						profile_score.innerHTML = 'Score: ' + user_score;

						if (user.what_i_would_like_to_do) {
							var profile_what_i_would_like_to_do = document.createElement('div');
							profile_what_i_would_like_to_do.innerHTML = user.what_i_would_like_to_do;
							userBox.appendChild(profile_what_i_would_like_to_do);
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
					page = data.page + 1;
				}
				busy = false;
			}
		} catch (err) {
			busy = false;
			throw err;
		}
	};

	var uri = "/data/newcomers.json";
	if (oldest_uid !== null)
		uri += "?before_user_id=" + oldest_uid;

	rq.open("GET", uri, true);
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
